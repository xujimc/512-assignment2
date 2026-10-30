package comp512st.paxos;

import java.io.Serializable;

class PaxosMsg implements Serializable
{
	enum Type { PREPARE, PROMISE, REFUSE, ACCEPT, ACCEPTED, REJECT, DECIDE, HEARTBEAT, DONE }

	final Type type;
	final int slot;
	final Ballot ballot;
	final Ballot otherBallot;
	final Proposal value;

	PaxosMsg(Type type, int slot, Ballot ballot, Ballot otherBallot, Proposal value)
	{
		this.type = type;
		this.slot = slot;
		this.ballot = ballot;
		this.otherBallot = otherBallot;
		this.value = value;
	}

	public String toString()
	{
		return type + "[slot=" + slot + ", ballot=" + ballot + ", other=" + otherBallot + ", value=" + value + "]";
	}
}
