/**
 * @author j16sdiz (1024D/75494252)
 */
package plugins.XMLSpider.db;

import plugins.XMLSpider.org.garret.perst.Persistent;
import plugins.XMLSpider.org.garret.perst.Storage;

public class TermPosition extends Persistent {
	/** Position List */
	public int[] positions;

	public TermPosition() {
	}
	
	public TermPosition(Storage storage, int[] positions){
		this.positions = positions;
		storage.makePersistent(this);
	}

	public TermPosition(Storage storage) {
		this(storage, new int[0]);
	}

	public synchronized void addPositions(int position) {
		int[] newPositions = new int[positions.length + 1];
		System.arraycopy(positions, 0, newPositions, 0, positions.length);
		newPositions[positions.length] = position;

		positions = newPositions;
		modify();
	}

	public synchronized int[] getPositions() {
		return positions;
	}
}