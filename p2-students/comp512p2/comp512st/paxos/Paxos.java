package comp512st.paxos;

// Access to the GCL layer
import comp512.gcl.*;

import comp512.utils.*;

// Any other imports that you may need.
import java.io.*;
import java.util.*;
import java.util.logging.*;
import java.net.UnknownHostException;


// ANY OTHER classes, etc., that you add must be private to this package and not visible to the application layer.

// extend / implement whatever interface, etc. as required.
// NO OTHER public members / methods allowed. broadcastTOMsg, acceptTOMsg, and shutdownPaxos must be the only visible methods to the application layer.
//		You should also not change the signature of these methods (arguments and return value) other aspects maybe changed with reasonable design needs.
public class Paxos
{
	static final long PHASE_TIMEOUT_MS = 1000;
	static final long RECOVERY_MS = 1000;
	static final long HEARTBEAT_MS = 300;
	static final long DEAD_MS = 4000;
	static final long SHUTDOWN_GIVEUP_MS = 10000;
	static final int BACKOFF_BASE_MS = 5;
	static final int BACKOFF_MAX_EXP = 6;

	GCL gcl;
	FailCheck failCheck;
	Logger logger;
	String me;
	String[] all;
	int majority;
	Random rand = new Random();

	Map<Integer, Ballot> promised = new HashMap<>();
	Map<Integer, Ballot> acceptedBallot = new HashMap<>();
	Map<Integer, Proposal> acceptedValue = new HashMap<>();

	Map<Integer, Proposal> decided = new HashMap<>();
	Set<String> deliveredIds = new HashSet<>();
	Deque<Object> deliveryQueue = new ArrayDeque<>();
	int nextDeliver = 1;
	int maxSlotSeen = 0;
	long lastSlotActivity = System.currentTimeMillis();
	boolean readerWaiting = false;

	int maxCounter = 0;
	int rSlot = -1;
	Ballot rBallot;
	int rPhase;
	Set<String> rVotes = new HashSet<>();
	Ballot rHighBallot;
	Proposal rHighValue;
	boolean rRefused;

	Proposal pending;
	long seq = 0;
	long noopSeq = 0;

	Map<String, Long> lastHeard = new HashMap<>();
	Set<String> doneSet = new HashSet<>();
	volatile boolean running = true;

	Thread receiver;
	Thread proposer;
	Thread heartbeat;

	public Paxos(String myProcess, String[] allGroupProcesses, Logger logger, FailCheck failCheck) throws IOException, UnknownHostException
	{
		// Rember to call the failCheck.checkFailure(..) with appropriate arguments throughout your Paxos code to force fail points if necessary.
		this.failCheck = failCheck;
		this.logger = logger;
		this.me = myProcess;
		this.all = allGroupProcesses.clone();
		this.majority = all.length / 2 + 1;

		long now = System.currentTimeMillis();
		for(String p : all)
			lastHeard.put(p, now);

		// Initialize the GCL communication system as well as anything else you need to.
		this.gcl = new GCL(myProcess, allGroupProcesses, null, logger) ;

		receiver = new Thread(this::receiveLoop, "paxos-receiver");
		proposer = new Thread(this::proposeLoop, "paxos-proposer");
		heartbeat = new Thread(this::heartbeatLoop, "paxos-heartbeat");
		receiver.setDaemon(true);
		proposer.setDaemon(true);
		heartbeat.setDaemon(true);
		receiver.start();
		proposer.start();
		heartbeat.start();
		logger.info("Paxos started: me=" + me + " group=" + Arrays.toString(all) + " majority=" + majority);
	}

	// This is what the application layer is going to call to send a message/value, such as the player and the move
	public void broadcastTOMsg(Object val)
	{
		boolean interrupted = false;
		synchronized(this)
		{
			Proposal p = new Proposal(me, ++seq, false, val);
			pending = p;
			logger.fine("Paxos broadcastTOMsg pending " + p);
			notifyAll();
			while(pending == p && running)
			{
				try { wait(); }
				catch(InterruptedException ie) { interrupted = true; }
			}
		}
		if(interrupted)
			Thread.currentThread().interrupt();
	}

	// This is what the application layer is calling to figure out what is the next message in the total order.
	// Messages delivered in ALL the processes in the group should deliver this in the same order.
	public synchronized Object acceptTOMsg() throws InterruptedException
	{
		readerWaiting = true;
		notifyAll();
		try
		{
			while(deliveryQueue.isEmpty())
				wait();
			return deliveryQueue.poll();
		}
		finally
		{
			readerWaiting = false;
		}
	}

	// Add any of your own shutdown code into this method.
	public void shutdownPaxos()
	{
		logger.info("Paxos shutdown requested");
		broadcast(new PaxosMsg(PaxosMsg.Type.DONE, 0, null, null, null));
		synchronized(this)
		{
			doneSet.add(me);
			long unsettledSince = -1;
			while(true)
			{
				long now = System.currentTimeMillis();
				boolean peersFinished = true;
				for(String p : all)
					if(!doneSet.contains(p) && now - lastHeard.get(p) <= DEAD_MS)
						peersFinished = false;
				boolean settled = maxSlotSeen < nextDeliver;
				boolean drained = deliveryQueue.isEmpty() && readerWaiting;
				if(peersFinished && settled && drained)
					break;
				if(peersFinished)
				{
					if(unsettledSince < 0)
						unsettledSince = now;
					else if(now - unsettledSince > SHUTDOWN_GIVEUP_MS)
					{
						logger.warning("Paxos shutdown giving up waiting: settled=" + settled + " drained=" + drained + " nextDeliver=" + nextDeliver + " maxSlotSeen=" + maxSlotSeen);
						break;
					}
				}
				else
					unsettledSince = -1;
				try { wait(50); }
				catch(InterruptedException ie) { break; }
			}
			running = false;
			notifyAll();
			logger.info("Paxos shutting down: delivered up to slot " + (nextDeliver - 1) + ", done=" + doneSet);
		}
		gcl.shutdownGCL();
		proposer.interrupt();
		heartbeat.interrupt();
	}

	void send(PaxosMsg m, String dest)
	{
		try { gcl.sendMsg(m, dest); }
		catch(IllegalStateException ise) { }
	}

	void broadcast(PaxosMsg m)
	{
		try { gcl.broadcastMsg(m); }
		catch(IllegalStateException ise) { }
	}

	void heartbeatLoop()
	{
		while(running)
		{
			broadcast(new PaxosMsg(PaxosMsg.Type.HEARTBEAT, 0, null, null, null));
			try { Thread.sleep(HEARTBEAT_MS); }
			catch(InterruptedException ie) { }
		}
	}

	void receiveLoop()
	{
		while(running)
		{
			GCMessage gcm;
			try { gcm = gcl.readGCMessage(); }
			catch(InterruptedException | IllegalStateException e) { break; }
			if(!(gcm.val instanceof PaxosMsg))
			{
				logger.warning("Paxos ignoring unknown message " + gcm);
				continue;
			}
			PaxosMsg m = (PaxosMsg) gcm.val;
			String src = gcm.senderProcess;
			synchronized(this)
			{
				lastHeard.put(src, System.currentTimeMillis());
			}
			switch(m.type)
			{
				case PREPARE: onPrepare(src, m); break;
				case ACCEPT: onAccept(src, m); break;
				case PROMISE:
				case REFUSE:
				case ACCEPTED:
				case REJECT: onResponse(src, m); break;
				case DECIDE: learn(m.slot, m.value); break;
				case DONE:
					synchronized(this) { doneSet.add(src); notifyAll(); }
					logger.info("Paxos got DONE from " + src);
					break;
				case HEARTBEAT: break;
			}
		}
	}

	void seeSlot(int slot, int counter)
	{
		if(slot > maxSlotSeen)
			maxSlotSeen = slot;
		if(slot == nextDeliver)
			lastSlotActivity = System.currentTimeMillis();
		if(counter > maxCounter)
			maxCounter = counter;
	}

	void onPrepare(String src, PaxosMsg m)
	{
		failCheck.checkFailure(FailCheck.FailureType.RECEIVEPROPOSE);
		PaxosMsg reply;
		boolean vote = true;
		synchronized(this)
		{
			seeSlot(m.slot, m.ballot.counter);
			Proposal d = decided.get(m.slot);
			Ballot p = promised.get(m.slot);
			if(d != null)
			{
				reply = new PaxosMsg(PaxosMsg.Type.DECIDE, m.slot, null, null, d);
				vote = false;
			}
			else if(p == null || m.ballot.compareTo(p) > 0)
			{
				promised.put(m.slot, m.ballot);
				reply = new PaxosMsg(PaxosMsg.Type.PROMISE, m.slot, m.ballot, acceptedBallot.get(m.slot), acceptedValue.get(m.slot));
			}
			else
				reply = new PaxosMsg(PaxosMsg.Type.REFUSE, m.slot, m.ballot, p, null);
		}
		send(reply, src);
		logger.fine("Paxos acceptor sent " + reply + " to " + src);
		if(vote)
			failCheck.checkFailure(FailCheck.FailureType.AFTERSENDVOTE);
	}

	void onAccept(String src, PaxosMsg m)
	{
		PaxosMsg reply;
		synchronized(this)
		{
			seeSlot(m.slot, m.ballot.counter);
			Proposal d = decided.get(m.slot);
			Ballot p = promised.get(m.slot);
			if(d != null)
				reply = new PaxosMsg(PaxosMsg.Type.DECIDE, m.slot, null, null, d);
			else if(p == null || m.ballot.compareTo(p) >= 0)
			{
				promised.put(m.slot, m.ballot);
				acceptedBallot.put(m.slot, m.ballot);
				acceptedValue.put(m.slot, m.value);
				reply = new PaxosMsg(PaxosMsg.Type.ACCEPTED, m.slot, m.ballot, null, null);
			}
			else
				reply = new PaxosMsg(PaxosMsg.Type.REJECT, m.slot, m.ballot, p, null);
		}
		send(reply, src);
		logger.fine("Paxos acceptor sent " + reply + " to " + src);
	}

	synchronized void onResponse(String src, PaxosMsg m)
	{
		seeSlot(m.slot, m.otherBallot == null ? m.ballot.counter : Math.max(m.ballot.counter, m.otherBallot.counter));
		if(rBallot == null || m.slot != rSlot || m.ballot.compareTo(rBallot) != 0)
			return;
		switch(m.type)
		{
			case PROMISE:
				if(rPhase != 1)
					return;
				rVotes.add(src);
				if(m.otherBallot != null && (rHighBallot == null || m.otherBallot.compareTo(rHighBallot) > 0))
				{
					rHighBallot = m.otherBallot;
					rHighValue = m.value;
				}
				break;
			case ACCEPTED:
				if(rPhase != 2)
					return;
				rVotes.add(src);
				break;
			case REFUSE:
			case REJECT:
				rRefused = true;
				break;
			default:
				return;
		}
		notifyAll();
	}

	synchronized void learn(int slot, Proposal value)
	{
		seeSlot(slot, 0);
		Proposal existing = decided.get(slot);
		if(existing != null)
		{
			if(!existing.id().equals(value.id()))
				logger.severe("Paxos SAFETY VIOLATION slot " + slot + " decided " + existing + " but got " + value);
			return;
		}
		decided.put(slot, value);
		promised.remove(slot);
		acceptedBallot.remove(slot);
		acceptedValue.remove(slot);
		logger.fine("Paxos decided slot " + slot + " = " + value);
		if(pending != null && pending.id().equals(value.id()))
		{
			logger.fine("Paxos own value " + value.id() + " chosen at slot " + slot);
			pending = null;
		}
		while(decided.containsKey(nextDeliver))
		{
			Proposal p = decided.get(nextDeliver);
			if(!p.noop && deliveredIds.add(p.id()))
				deliveryQueue.add(p.val);
			nextDeliver++;
			lastSlotActivity = System.currentTimeMillis();
		}
		notifyAll();
	}

	void proposeLoop()
	{
		int failures = 0;
		while(running)
		{
			int slot;
			Proposal v;
			synchronized(this)
			{
				slot = nextDeliver;
				if(pending != null)
					v = pending;
				else if(maxSlotSeen >= slot && System.currentTimeMillis() - lastSlotActivity > RECOVERY_MS)
				{
					v = new Proposal(me, ++noopSeq, true, null);
					logger.info("Paxos recovery: driving stalled slot " + slot + " (maxSlotSeen=" + maxSlotSeen + ")");
				}
				else
				{
					try { wait(50); }
					catch(InterruptedException ie) { }
					continue;
				}
			}
			if(runRound(slot, v))
				failures = 0;
			else
			{
				failures++;
				int bound = BACKOFF_BASE_MS << Math.min(failures, BACKOFF_MAX_EXP);
				try { Thread.sleep(rand.nextInt(bound) + 1); }
				catch(InterruptedException ie) { }
			}
		}
	}

	synchronized boolean awaitVotes(int slot)
	{
		long deadline = System.currentTimeMillis() + PHASE_TIMEOUT_MS;
		while(running && rVotes.size() < majority && !rRefused && slot >= nextDeliver && !decided.containsKey(slot))
		{
			long left = deadline - System.currentTimeMillis();
			if(left <= 0)
				break;
			try { wait(left); }
			catch(InterruptedException ie) { }
		}
		return rVotes.size() >= majority;
	}

	synchronized boolean slotResolved(int slot)
	{
		return slot < nextDeliver || decided.containsKey(slot);
	}

	synchronized void endRound()
	{
		rBallot = null;
		rSlot = -1;
		rPhase = 0;
	}

	boolean runRound(int slot, Proposal v)
	{
		Ballot b;
		synchronized(this)
		{
			b = new Ballot(++maxCounter, me);
			rSlot = slot;
			rBallot = b;
			rPhase = 1;
			rVotes.clear();
			rHighBallot = null;
			rHighValue = null;
			rRefused = false;
		}
		logger.fine("Paxos proposer PREPARE slot " + slot + " ballot " + b + " for " + v.id());
		broadcast(new PaxosMsg(PaxosMsg.Type.PREPARE, slot, b, null, null));
		failCheck.checkFailure(FailCheck.FailureType.AFTERSENDPROPOSE);

		boolean ok = awaitVotes(slot);
		if(slotResolved(slot))
		{
			endRound();
			return true;
		}
		if(!ok)
		{
			logger.fine("Paxos proposer lost phase 1 slot " + slot + " ballot " + b);
			endRound();
			return false;
		}

		Proposal chosen;
		synchronized(this)
		{
			chosen = rHighValue != null ? rHighValue : v;
		}
		logger.fine("Paxos proposer became leader slot " + slot + " ballot " + b + " proposing " + chosen.id());
		failCheck.checkFailure(FailCheck.FailureType.AFTERBECOMINGLEADER);

		synchronized(this)
		{
			rPhase = 2;
			rVotes.clear();
			rRefused = false;
		}
		broadcast(new PaxosMsg(PaxosMsg.Type.ACCEPT, slot, b, null, chosen));

		ok = awaitVotes(slot);
		if(slotResolved(slot))
		{
			endRound();
			return true;
		}
		endRound();
		if(!ok)
		{
			logger.fine("Paxos proposer lost phase 2 slot " + slot + " ballot " + b);
			return false;
		}

		logger.fine("Paxos proposer majority accepted slot " + slot + " ballot " + b + " value " + chosen.id());
		failCheck.checkFailure(FailCheck.FailureType.AFTERVALUEACCEPT);
		learn(slot, chosen);
		broadcast(new PaxosMsg(PaxosMsg.Type.DECIDE, slot, null, null, chosen));
		return true;
	}
}
