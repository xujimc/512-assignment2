package comp512st.paxos;

// Access to the GCL layer
import comp512.gcl.*;

import comp512.utils.*;

// Any other imports that you may need.
import java.io.*;
import java.util.logging.*;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BooleanSupplier;


// ANY OTHER classes, etc., that you add must be private to this package and not visible to the application layer.

// extend / implement whatever interface, etc. as required.
// NO OTHER public members / methods allowed. broadcastTOMsg, acceptTOMsg, and shutdownPaxos must be the only visible methods to the application layer.
//		You should also not change the signature of these methods (arguments and return value) other aspects maybe changed with reasonable design needs.
public class Paxos
{
	private static final long PHASE_TIMEOUT_MS = 1000;

	GCL gcl;
	FailCheck failCheck;
	private final String myProcess;
	private final int numProcesses;
	private final int majority;
	private int round = 0;
	private final Map<Integer, Object> decided = new HashMap<>();
	private final Set<String> decidedIds = new HashSet<>();
	private int moveSeq = 0;
	private int lowestUndecided = 1;
	private int nextDeliver = 1;
	private int currentSlot;
	private Ballot currentBallot;
	private int promiseCount;
	private int ackCount;
	private final Set<String> refusers = new HashSet<>();
	private Ballot highestOldBallot;
	private Object highestOldValue;
	private final Map<Integer, AcceptorState> acceptorStates = new HashMap<>();
	private volatile boolean running = true;
	private final Thread listener;

	private static class AcceptorState
	{
		Ballot promisedBallot;
		Ballot acceptedBallot;
		Object acceptedValue;
	}

	public Paxos(String myProcess, String[] allGroupProcesses, Logger logger, FailCheck failCheck) throws IOException, UnknownHostException
	{
		// Rember to call the failCheck.checkFailure(..) with appropriate arguments throughout your Paxos code to force fail points if necessary.
		this.failCheck = failCheck;
		this.myProcess = myProcess;
		this.numProcesses = allGroupProcesses.length;
		this.majority = numProcesses / 2 + 1;

		// Initialize the GCL communication system as well as anything else you need to.
		this.gcl = new GCL(myProcess, allGroupProcesses, null, logger) ;

		this.listener = new Thread(this::listen);
		this.listener.setDaemon(true);
		this.listener.start();
	}

	private synchronized int nextSlot()
	{
		while (decided.containsKey(lowestUndecided))
			lowestUndecided++;
		return lowestUndecided;
	}

	private synchronized AcceptorState acceptorState(int slot)
	{
		return acceptorStates.computeIfAbsent(slot, k -> new AcceptorState());
	}

	private boolean isCurrentAttempt(PaxosMsg msg)
	{
		return currentBallot != null && msg.slot == currentSlot && msg.ballot.compareTo(currentBallot) == 0;
	}

	private void listen()
	{
		while (running)
		{
			GCMessage gcmsg;
			try
			{
				gcmsg = gcl.readGCMessage();
			}
			catch (InterruptedException e)
			{
				return;
			}
			if (!(gcmsg.val instanceof PaxosMsg))
				continue;
			PaxosMsg msg = (PaxosMsg) gcmsg.val;
			String sender = gcmsg.senderProcess;
			switch (msg.type)
			{
				case PROPOSE:
					failCheck.checkFailure(FailCheck.FailureType.RECEIVEPROPOSE);
					PaxosMsg vote;
					synchronized (this)
					// check if incoming is bigger than current leader
					{
						AcceptorState st = acceptorState(msg.slot);
						if (st.promisedBallot == null || msg.ballot.compareTo(st.promisedBallot) > 0)
						{
							st.promisedBallot = msg.ballot;
							vote = PaxosMsg.promise(msg.slot, msg.ballot, st.acceptedBallot, st.acceptedValue);
						}
						else
						{
							vote = PaxosMsg.refuse(msg.slot, msg.ballot, st.promisedBallot);
						}
					}
					gcl.sendMsg(vote, sender);
					failCheck.checkFailure(FailCheck.FailureType.AFTERSENDVOTE);
					break;
				case PROMISE:
					synchronized (this)
					{
						if (isCurrentAttempt(msg))
						{
							promiseCount++;
							// this is when we make sure to write the previous guy's value
							if (msg.oldBallot != null && (highestOldBallot == null || msg.oldBallot.compareTo(highestOldBallot) > 0))
							{
								highestOldBallot = msg.oldBallot;
								highestOldValue = msg.val;
							}
							notifyAll();
						}
					}
					break;
				case REFUSE:
					synchronized (this)
					{
						if (isCurrentAttempt(msg))
						{
							refusers.add(sender);
							round = Math.max(round, msg.oldBallot.round);
							notifyAll();
						}
					}
					break;
				case ACCEPT_REQ:
					PaxosMsg reply;
					synchronized (this)
					{
						AcceptorState st = acceptorState(msg.slot);
						if (st.promisedBallot != null && msg.ballot.compareTo(st.promisedBallot) == 0)
						{
							st.acceptedBallot = msg.ballot;
							st.acceptedValue = msg.val;
							reply = PaxosMsg.acceptAck(msg.slot, msg.ballot, msg.val);
						}
						else
						{
							reply = PaxosMsg.refuse(msg.slot, msg.ballot, st.promisedBallot);
						}
					}
					gcl.sendMsg(reply, sender);
					break;
				case ACCEPT_ACK:
					synchronized (this)
					{
						if (isCurrentAttempt(msg))
						{
							ackCount++;
							notifyAll();
						}
					}
					break;
				case CONFIRM:

				// this is when we commit a value
					markDecided(msg.slot, msg.val);
					break;
				case ERROR:
					break;
			}
		}
	}

	private synchronized void markDecided(int slot, Object v)
	{
		if (decided.putIfAbsent(slot, v) == null && v instanceof Proposal)
			decidedIds.add(((Proposal) v).id);
		notifyAll();
	}

	private boolean lost(int slot)
	{
		return refusers.size() > numProcesses - majority || decided.containsKey(slot);
	}

	private synchronized boolean attemptLost(int slot)
	{
		return lost(slot);
	}

	private synchronized boolean isMoveDecided(String id)
	{
		return decidedIds.contains(id);
	}

	private void backoff()
	{
		try
		{
			Thread.sleep(ThreadLocalRandom.current().nextInt(50));
		}
		catch (InterruptedException e)
		{
			Thread.currentThread().interrupt();
		}
	}

	private synchronized boolean waitFor(BooleanSupplier cond)
	{
		long deadline = System.currentTimeMillis() + PHASE_TIMEOUT_MS;
		while (!cond.getAsBoolean())
		{
			long left = deadline - System.currentTimeMillis();
			if (left <= 0)
				return false;
			try
			{
				wait(left);
			}
			catch (InterruptedException e)
			{
				Thread.currentThread().interrupt();
				return false;
			}
		}
		return true;
	}

	// This is what the application layer is going to call to send a message/value, such as the player and the move
	public void broadcastTOMsg(Object val)
	{
		Proposal mine;
		synchronized (this)
		{
			mine = new Proposal(myProcess + "#" + (++moveSeq), val);
		}
		while (!isMoveDecided(mine.id))
		{
			int slot = nextSlot();
			Ballot ballot;
			synchronized (this)
			{
				ballot = new Ballot(++round, myProcess);
				currentSlot = slot;
				currentBallot = ballot;
				promiseCount = 0;
				ackCount = 0;
				highestOldBallot = null;
				highestOldValue = null;
				refusers.clear();
			}
			gcl.broadcastMsg(PaxosMsg.propose(slot, ballot));
			failCheck.checkFailure(FailCheck.FailureType.AFTERSENDPROPOSE);

			if (!waitFor(() -> promiseCount >= majority || lost(slot)))
			{
				if (Thread.currentThread().isInterrupted())
					return;
				backoff();
				continue;
			}
			if (attemptLost(slot))
			{
				backoff();
				continue;
			}
			failCheck.checkFailure(FailCheck.FailureType.AFTERBECOMINGLEADER);

			Object chosen;
			synchronized (this)
			{
				chosen = highestOldValue != null ? highestOldValue : mine;
			}
			gcl.broadcastMsg(PaxosMsg.acceptReq(slot, ballot, chosen));

			if (!waitFor(() -> ackCount >= majority || lost(slot)))
			{
				if (Thread.currentThread().isInterrupted())
					return;
				backoff();
				continue;
			}
			if (attemptLost(slot))
			{
				backoff();
				continue;
			}
			failCheck.checkFailure(FailCheck.FailureType.AFTERVALUEACCEPT);
			markDecided(slot, chosen);
			gcl.broadcastMsg(PaxosMsg.confirm(slot, ballot, chosen));
		}
	}

	// This is what the application layer is calling to figure out what is the next message in the total order.
	// Messages delivered in ALL the processes in the group should deliver this in the same order.
	public synchronized Object acceptTOMsg() throws InterruptedException
	{
		while (!decided.containsKey(nextDeliver))
			wait();
		return ((Proposal) decided.get(nextDeliver++)).val;
	}

	// Add any of your own shutdown code into this method.
	public void shutdownPaxos()
	{
		running = false;
		listener.interrupt();
		gcl.shutdownGCL();
	}
}
