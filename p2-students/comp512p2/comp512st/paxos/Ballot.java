package comp512st.paxos;

import java.io.Serializable;

class Ballot implements Serializable, Comparable<Ballot>
{
	final int round;
	final String processId;

	Ballot(int round, String processId)
	{
		this.round = round;
		this.processId = processId;
	}

	@Override
	public int compareTo(Ballot o)
	{
		if (round != o.round)
			return Integer.compare(round, o.round);
		return processId.compareTo(o.processId);
	}

	@Override
	public String toString()
	{
		return "(" + round + "," + processId + ")";
	}
}
