package comp512st.paxos;

import java.io.Serializable;

class Ballot implements Serializable
{
	final int counter;
	final String processId;

	Ballot(int counter, String processId)
	{
		this.counter = counter;
		this.processId = processId;
	}

	int compareTo(Ballot o)
	{
		if(counter != o.counter)
			return Integer.compare(counter, o.counter);
		return processId.compareTo(o.processId);
	}

	public String toString()
	{
		return counter + "@" + processId;
	}
}
