package comp512st.paxos;

import java.io.Serializable;
import java.util.Arrays;

class Proposal implements Serializable
{
	final String origin;
	final long seq;
	final boolean noop;
	final Object val;

	Proposal(String origin, long seq, boolean noop, Object val)
	{
		this.origin = origin;
		this.seq = seq;
		this.noop = noop;
		this.val = val;
	}

	String id()
	{
		return (noop ? "noop:" : "") + origin + ":" + seq;
	}

	public String toString()
	{
		return id() + (val instanceof Object[] ? Arrays.toString((Object[]) val) : String.valueOf(val));
	}
}
