package comp512st.paxos;

import java.io.Serializable;

class PaxosMsg implements Serializable
{
	enum Type { PROPOSE, PROMISE, REFUSE, ACCEPT_REQ, ACCEPT_ACK, CONFIRM, ERROR }

	final Type type;
	final int slot;
	final Ballot ballot;
	final Object val;
	final Ballot oldBallot;

	private PaxosMsg(Type type, int slot, Ballot ballot, Object val, Ballot oldBallot)
	{
		this.type = type;
		this.slot = slot;
		this.ballot = ballot;
		this.val = val;
		this.oldBallot = oldBallot;
	}

	static PaxosMsg propose(int slot, Ballot ballot)
	{
		return new PaxosMsg(Type.PROPOSE, slot, ballot, null, null);
	}

	static PaxosMsg promise(int slot, Ballot ballot, Ballot oldBallot, Object oldValue)
	{
		return new PaxosMsg(Type.PROMISE, slot, ballot, oldValue, oldBallot);
	}

	static PaxosMsg refuse(int slot, Ballot ballot, Ballot promisedBallot)
	{
		return new PaxosMsg(Type.REFUSE, slot, ballot, null, promisedBallot);
	}

	static PaxosMsg error(int slot, Ballot ballot, Object val)
	{
		return new PaxosMsg(Type.ERROR, slot, ballot, val, null);
	}

	static PaxosMsg acceptReq(int slot, Ballot ballot, Object val)
	{
		return new PaxosMsg(Type.ACCEPT_REQ, slot, ballot, val, null);
	}

	static PaxosMsg acceptAck(int slot, Ballot ballot, Object val)
	{
		return new PaxosMsg(Type.ACCEPT_ACK, slot, ballot, val, null);
	}

	static PaxosMsg confirm(int slot, Ballot ballot, Object val)
	{
		return new PaxosMsg(Type.CONFIRM, slot, ballot, val, null);
	}

	@Override
	public String toString()
	{
		return type + " slot=" + slot + " b=" + ballot + (oldBallot != null ? " old=" + oldBallot : "") + (val != null ? " val=" + val : "");
	}
}
