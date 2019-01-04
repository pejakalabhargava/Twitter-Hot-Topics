package storm.starter.trident.project.functions;

import java.io.Serializable;

/**
 * Class representing a tweet.
 */
public class Tweet implements Serializable,Comparable  {

    private String text;
    private int count;

    public Tweet() {
    }

    public Tweet(String text, int count) {
        this.text = text;
        this.count = count;
    }

    public String getText() {
        return text;
    }

    public int getCount() {
        return count;
    }

    public void incrementCount( ) {
        this.count++;
    }

    @Override
	public String toString() {
		return "Tweet [text=" + text + ", count=" + count + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((text == null) ? 0 : text.hashCode());
		return result;
	}

	@Override
	/**
	 * Impl of equals
	 */
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Tweet other = (Tweet) obj;
		if (text == null) {
			if (other.text != null)
				return false;
		} else if (!text.equals(other.text))
			return false;
		return true;
	}
	/**
	 * This compares the tweets so that when its added to priority queue its handled
	 */
	@Override
	public int compareTo(Object otherTweet) {
		Tweet tweet = (Tweet) otherTweet;
		return (this.count < tweet.count) ? 1 : (this.count > tweet.count) ? -1: 0;
	}
    
}
