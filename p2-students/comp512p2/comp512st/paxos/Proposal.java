package comp512st.paxos;

import java.io.Serializable;

class Proposal implements Serializable
{
	final String id;
	final Object val;

	Proposal(String id, Object val)
	{
		this.id = id;
		this.val = val;
	}

	@Override
	public String toString()
	{
		return id;
	}
}
