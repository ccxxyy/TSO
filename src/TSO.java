import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.util.Scanner;
import java.util.Vector;

/**
 * the class used to store the information and of each address in the Memory
 * 
 * @param address
 *            is the address of the processor visit
 * @param Read
 *            is whether such address has been read
 * @param Write
 *            is whether such address has been write
 * @param Processor
 *            is a vector to store the processors that access to such memory
 *            address
 * @param VisiTimes
 *            is the how may times such address has been visited
 */
class MemoryLines {
	private String address;
	private boolean Read;
	private boolean Write;
	private Vector<String> Processor;
	private int VisitTimes;

	public MemoryLines(String address, boolean Read, boolean Write,
			Vector<String> Processor, int VistTimes) {
		this.address = address;
		this.Read = Read;
		this.Write = Write;
		this.Processor = Processor;
		this.VisitTimes = VistTimes;
	}

	public String getAddress() {
		return address;
	}

	public boolean getRead() {
		return Read;
	}

	public boolean getWrite() {
		return Write;
	}

	public void setRead() {
		Read = true;
	}

	public void setWrite() {
		Write = true;
	}

	public Vector<String> getProcessor() {
		return Processor;
	}

	public void setVisitTimes() {
		VisitTimes = VisitTimes + 1;
	}

	public int getVisitTimes() {
		return VisitTimes;
	}
}

/**
 * this is a class to store the memory lines
 * 
 * @param lines
 *            is the Vector stores all the memory lines
 * 
 */

class Memory {
	private Vector<MemoryLines> lines;

	public Memory(Vector<MemoryLines> lines) {
		this.lines = lines;
	}

	public Vector<MemoryLines> getLines() {
		return lines;
	}
}

/**
 * Construct a new class to store the information of each cache line
 * 
 * @param Tag
 *            is the tag of such cache line
 * @param Index
 *            is the index of such cache line
 * @param Status
 *            is the status of such cache line, includes Shared Modified and
 *            Invalid
 * 
 */
class CacheLine {
	private String Tag;
	private String Index;
	private String Offset;
	private String Status;

	public CacheLine(String Tag, String Index, String Offset, String Status) {
		this.Tag = Tag;
		this.Index = Index;
		this.Offset = Offset;
		this.Status = Status;
	}

	public String getTag() {
		return Tag;
	}

	public String getIndex() {
		return Index;
	}

	public String getOffset() {
		return Offset;
	}

	public String getStatus() {
		return Status;
	}

	public void setStatus(String a) {
		Status = a;
	}
}

/**
 * 
 * This is a class to store all the cache lines, like the class Memory
 * 
 * @param cacheline
 *            is a vector to store all the cache lines
 * 
 */

class Cache {
	private Vector<CacheLine> cacheLine;

	public Cache(Vector<CacheLine> cacheLine) {
		this.cacheLine = cacheLine;
	}

	public Vector<CacheLine> getCacheLine() {
		return cacheLine;
	}
}
/**
 * This is a simulator for buffer
 * 
 * @param size is the buffer size of the buffer
 * @param buffer is a vector store the things in real buffer
 *
 */
class buffer {
	private int size;
	private Vector<Vector> buffer;

	public buffer(int size, Vector<Vector> buffer) {
		this.size = size;
		this.buffer = buffer;
	}

	public Vector<Vector> getBuffer() {
		return buffer;
	}

}

/** a class to store some often used methods */
class Method {

	public Method() {

	}

	/** a method deal with the read command */
	public void read(Cache Cache, String tag, String index, String offset,
			int localLine) {
		// int TotalMiss=0;
		// int latency=0;
		/**
		 * if the corresponding cache line is empty, remove the empty line and
		 * change the line status to shared
		 */
		if (Cache.getCacheLine().elementAt(localLine).getTag().equals("Empty")) {
			// System.out.println("ReadEEEEEEMPTY");
			CacheLine newLines = new CacheLine(tag, index, offset, "Shared");
			Cache.getCacheLine().removeElementAt(localLine);
			Cache.getCacheLine().add(localLine, newLines);
			// latency=latency+200;
			// TotalMiss++;
		}
		/**
		 * if the corresponding cache line has correct tag, and the status is
		 * not invalid, then this is a read hit
		 */
		else if (Cache.getCacheLine().elementAt(localLine).getTag().equals(tag)
				&& !Cache.getCacheLine().elementAt(localLine).getStatus()
						.equals("Invalid")) {
			// latency=latency+2;
			// System.out.println("Readsame");
			// ReadHit++;
		}
		/**
		 * other situation means a read miss remove the old cache line and add
		 * the new one
		 */
		else {

	
			CacheLine newLines = new CacheLine(tag, index, offset, "Shared");
			Cache.getCacheLine().removeElementAt(localLine);
			Cache.getCacheLine().add(localLine, newLines);
			// latency=latency+200;
			// TotalMiss++;
		}
		// return latency;
		// System.out.println("hit is: "+hit);
	}

	/** the method deal with command W */
	public int write(Cache Cache, String tag, String index, String offset,
			int localLine) {
		int WriteMiss = 0;
		/** if the line is empty, remove emply line and add the new one */
		if (Cache.getCacheLine().elementAt(localLine).getTag().equals("Empty")) {
			// System.out.println("WriteEmpty");
			CacheLine newLines = new CacheLine(tag, index, offset, "Modified");
			Cache.getCacheLine().removeElementAt(localLine);
			Cache.getCacheLine().add(localLine, newLines);
			WriteMiss++;
			// TotalMiss++;
		}

		/** if has correct tag, directly write and change to modified status */
		else if (Cache.getCacheLine().elementAt(localLine).getTag().equals(tag)
				&& !Cache.getCacheLine().elementAt(localLine).getStatus()
						.equals("Invalid")) {
			// System.out.println("Writesame");
			if (Cache.getCacheLine().elementAt(localLine).getStatus()
					.equals("Shared")) {
				WriteMiss++;
				// TotalMiss++;
			}
			Cache.getCacheLine().elementAt(localLine).setStatus("Modified");
		}
		/** when the status is modified, directly write and this is a write hit */
		else {

			// if(Cache.getCacheLine().elementAt(localLine).getTag().equals(tag)&&
			// Cache.getCacheLine().elementAt(localLine).getStatus().equals("Invalid")){
			// CoherenceMiss++;
			// }

			// System.out.println("WriteChange");
			CacheLine newLines = new CacheLine(tag, index, offset, "Modified");
			Cache.getCacheLine().removeElementAt(localLine);
			Cache.getCacheLine().add(localLine, newLines);
			WriteMiss++;
			// TotalMiss++;
		}
		return WriteMiss;

	}

	/**
	 * this method will be invoked when a cache want to check whether other 3 three cache
	 * has the copy of what it want
	 */
	public boolean checkRead(Cache a, Cache b, Cache c, String x, int y) {

		if (a.getCacheLine().elementAt(y).getTag().equals(x)
				&& !a.getCacheLine().elementAt(y).getStatus().equals("Invalid")) {
			return true;
		}
		if (b.getCacheLine().elementAt(y).getTag().equals(x)
				&& !b.getCacheLine().elementAt(y).getStatus().equals("Invalid")) {
			return true;
		}
		if (c.getCacheLine().elementAt(y).getTag().equals(x)
				&& !c.getCacheLine().elementAt(y).getStatus().equals("Invalid")) {
			return true;
		}
		return false;

	}

	/**
	 * method used to check whether other caches's line should change status to
	 * invalid, when a processor has a write operation
	 */
	public void checkInvalid(Cache a, Cache b, Cache c, String x, int y) {
		if (a.getCacheLine().elementAt(y).getTag().equals(x)) {
			// System.out.println("invalid");
			a.getCacheLine().elementAt(y).setStatus("Invalid");
		}
		if (b.getCacheLine().elementAt(y).getTag().equals(x)) {
			// System.out.println("invalid");
			b.getCacheLine().elementAt(y).setStatus("Invalid");
		}
		if (c.getCacheLine().elementAt(y).getTag().equals(x)) {
			// System.out.println("invalid");
			c.getCacheLine().elementAt(y).setStatus("Invalid");
		}
	}

	/**
	 * when one cache read a address, which is obtained by only one caches and
	 * the cache line status in that line is modified, this method will be
	 * invoked and change the status to shared
	 */
	public void checkModified(Cache a, Cache b, Cache c, String x, int y) {
		if (a.getCacheLine().elementAt(y).getTag().equals(x)
				&& a.getCacheLine().elementAt(y).getStatus().equals("Modified")) {
			// System.out.println("changeto share");
			a.getCacheLine().elementAt(y).setStatus("Shared");
		}
		if (b.getCacheLine().elementAt(y).getTag().equals(x)
				&& b.getCacheLine().elementAt(y).getStatus().equals("Modified")) {
			// System.out.println("changeto share");
			b.getCacheLine().elementAt(y).setStatus("Shared");
		}
		if (c.getCacheLine().elementAt(y).getTag().equals(x)
				&& c.getCacheLine().elementAt(y).getStatus().equals("Modified")) {
			// System.out.println("changeto share");
			c.getCacheLine().elementAt(y).setStatus("Shared");
		}
	}

	/** Method to set memory lines */
	public void setMemory(Memory mem, String tag, String index,
			String processor, String command) {
		int flag1 = 0;
		int flag = 0;
		for (int i = 0; i < mem.getLines().size(); i++) {
			if (mem.getLines().elementAt(i).getAddress().equals(tag + index)) {
				// System.out.println("sssssssssssssssss");
				// System.out.println(mem.getLines().elementAt(i).getVisitTimes());
				// System.out.println("sssssssssssssssss");
				if (command.equals("W")) {
					mem.getLines().elementAt(i).setWrite();
				} else if (command.equals("R")) {
					mem.getLines().elementAt(i).setRead();
				}

				for (int j = 0; j < mem.getLines().elementAt(i).getProcessor()
						.size(); j++) {
					if (mem.getLines().elementAt(i).getProcessor().elementAt(j)
							.equals(processor)) {
						flag = 1;
						// System.out.println("sameeee");
					}
				}
				if (flag == 0) {
					mem.getLines().elementAt(i).getProcessor().add(processor);
				}
				flag1 = 1;
				mem.getLines().elementAt(i).setVisitTimes();
				// System.out.println(mem.getLines().elementAt(i).getVisitTimes());
			}
		}
		if (flag1 == 0) {
			// System.out.println("nnnnnnnnnnnnnnnn");
			Vector<String> temp = new Vector<String>();
			temp.add(processor);
			if (command.equals("W")) {
				mem.getLines().add(
						new MemoryLines(tag + index, false, true, temp, 1));
			} else if (command.equals("R")) {
				mem.getLines().add(
						new MemoryLines(tag + index, true, false, temp, 1));
			}

		}
	}

}

class TSO {
	public static void main(String[] args) {

		/** initialize memory class */
		Memory mem = new Memory(new Vector<MemoryLines>());
		Vector<CacheLine> Empty0 = new Vector<CacheLine>();
		Vector<CacheLine> Empty1 = new Vector<CacheLine>();
		Vector<CacheLine> Empty2 = new Vector<CacheLine>();
		Vector<CacheLine> Empty3 = new Vector<CacheLine>();

		System.out.println("How many lines you want in the cache: ");
		Scanner sc = new Scanner(System.in);
		int lines = Integer.parseInt(sc.nextLine());
		System.out
				.println("What is the line size (How many words can be stored in a cache line): ");
		Scanner sc1 = new Scanner(System.in);
		int WordsOfLine = Integer.parseInt(sc1.nextLine());
		System.out.println("What is the buffer size: ");
		Scanner sc2 = new Scanner(System.in);
		int bufferSize = Integer.parseInt(sc2.nextLine());
		System.out.println("What number you want to retire: ");
		Scanner sc3 = new Scanner(System.in);
		int retire = Integer.parseInt(sc3.nextLine());

		System.out.println("Simulating... Please wait...");

		buffer buffer0 = new buffer(bufferSize, new Vector<Vector>());
		buffer buffer1 = new buffer(bufferSize, new Vector<Vector>());
		buffer buffer2 = new buffer(bufferSize, new Vector<Vector>());
		buffer buffer3 = new buffer(bufferSize, new Vector<Vector>());

		// int WordsOfLine = 4;
		// int lines = 128;
		/** count how many lines in the file */
		int count = 1;
		/** times of each processor read and write */
		int P0R = 0;
		int P1R = 0;
		int P2R = 0;
		int P3R = 0;
		int P0W = 0;
		int P1W = 0;
		int P2W = 0;
		int P3W = 0;

		int countP0 = 0;
		int countP1 = 0;
		int countP2 = 0;
		int countP3 = 0;

		/** count for every kinds of miss */
		int Cache0ReadMiss = 0;
		int Cache0WriteMiss = 0;
		int Cache0TotalMiss = 0;
		int Cache1ReadMiss = 0;
		int Cache1WriteMiss = 0;
		int Cache1TotalMiss = 0;
		int Cache2ReadMiss = 0;
		int Cache2WriteMiss = 0;
		int Cache2TotalMiss = 0;
		int Cache3ReadMiss = 0;
		int Cache3WriteMiss = 0;
		int Cache3TotalMiss = 0;

		int P0CoherenceMiss = 0;
		int P1CoherenceMiss = 0;
		int P2CoherenceMiss = 0;
		int P3CoherenceMiss = 0;

		int cacheHit = 2;
		int busTransaction = 20;
		int accessMemory = 200;
		int readBypass = 1;

		int latency0 = 0;
		int latency1 = 0;
		int latency2 = 0;
		int latency3 = 0;
		int sign = 0;

		// Vector<Vector> memory = new Vector<Vector>();
		/** add empty value to different caches */
		for (int i = 0; i < lines; i++) {
			Empty0.add(new CacheLine("Empty", "Empty", "Empty", "Invalid"));
			Empty1.add(new CacheLine("Empty", "Empty", "Empty", "Invalid"));
			Empty2.add(new CacheLine("Empty", "Empty", "Empty", "Invalid"));
			Empty3.add(new CacheLine("Empty", "Empty", "Empty", "Invalid"));
		}

		Cache cache0 = new Cache(Empty0);
		Cache cache1 = new Cache(Empty1);
		Cache cache2 = new Cache(Empty2);
		Cache cache3 = new Cache(Empty3);
		Method method = new Method();

		int hideLatency0 = 0;
		int hideLatency1 = 0;
		int hideLatency2 = 0;
		int hideLatency3 = 0;

		/** read the files */
		File file = new File("trace1.out.txt");
		if (file.isFile() && file.exists()) {
			try {
				InputStreamReader read = new InputStreamReader(
						new FileInputStream(file), "UTF-8");
				BufferedReader reader = new BufferedReader(read);
				String line;
				try {
					while ((line = reader.readLine()) != null) {

						/** change the address to binary system */
						// System.out.println("babdabadbd");
						String[] aa = line.split(" ");
						BigInteger src = new BigInteger(aa[2]);
						int CacheSize = (int) (Math.log(WordsOfLine * lines) / Math
								.log(2));
						String add = src.toString(2);
						while (add.length() < CacheSize + 1) {
							add = "0" + add;
						}
						// System.out.println(add);
						int NoOfoffset = (int) (Math.log(WordsOfLine) / Math
								.log(2));
						// System.out.println(NoOfoffset);
						String offset = add.substring(
								add.length() - NoOfoffset, add.length());

						int NoOfIndex = CacheSize - NoOfoffset;
						String index = add.substring(add.length() - NoOfoffset
								- NoOfIndex, add.length() - NoOfoffset);
						String tag = add.substring(0, add.length() - NoOfoffset
								- NoOfIndex);
						// System.out.println(index);

						/** localline shows the lines number in the cache. */
						BigInteger mid = new BigInteger(index, 2);
						int localLine = Integer.parseInt(mid.toString());

						/**
						 * due to different content of lines, we need to have
						 * different operation
						 */
						// System.out.println("line is: "+localLine);
						if (aa[0].equals("P0")) {

							// countP0++;
							if (aa[1].equals("R")) {
								// P0R++;
								int flag = 0;
								if (buffer0.getBuffer().size() != 0) {
									
									/** it need to check its buffer first when there is a read command*/
									for (int i = 0; i < buffer0.getBuffer()
											.size(); i++) {

										if ((tag + index).equals((String)buffer0
												.getBuffer().elementAt(i).elementAt(0)+(String)buffer0
												.getBuffer().elementAt(i).elementAt(1))) {
//											System.out.println("ddd");
											latency0 = latency0 + readBypass;
											flag = 1;
										}
									}
								}
								/** if it is not in its buffer */
								if (flag == 0) {
									if (cache0.getCacheLine()
											.elementAt(localLine).getTag()
											.equals(tag)
											&& !cache0.getCacheLine()
													.elementAt(localLine)
													.getStatus()
													.equals("Invalid")) {
										// System.out.println("Coherence missssssssssssssssss");
										// P0CoherenceMiss++;
										latency0 = latency0 + cacheHit;
									} else {
										if (method.checkRead(cache1, cache2,
												cache3, tag, localLine) == true) {
											latency0 = latency0
													+ busTransaction + cacheHit;
										} else {
											latency0 = latency0
													+ busTransaction
													+ accessMemory + cacheHit;
										}
									}

									method.read(cache0, tag, index, offset,
											localLine);
									method.checkModified(cache1, cache2,
											cache3, tag, localLine);
									method.setMemory(mem, tag, index, aa[0],
											aa[1]);
								}

								flag = 0;
								
								/** if the hide latency is enough to remove a line in buffer, then remove the 
								 * first line of buffer */
								if (latency0 >= hideLatency0
										&& hideLatency0 != 0) {
									// if(buffer0.getBuffer().size()!=0){
									buffer0.getBuffer().removeElementAt(0);
									// sign=0;
									if (buffer0.getBuffer().size() >= retire) {
										// sign=1;
										hideLatency0 = latency0
												+ busTransaction + accessMemory
												+ cacheHit;
										BigInteger temp1 = new BigInteger(
												(String) buffer0.getBuffer()
														.elementAt(0)
														.elementAt(1), 2);
										int ll = Integer.parseInt(temp1
												.toString());
										Cache0WriteMiss += method.write(cache0,
												(String) buffer0.getBuffer()
														.elementAt(0)
														.elementAt(0),
												(String) buffer0.getBuffer()
														.elementAt(0)
														.elementAt(1), "0", ll);
										method.checkInvalid(cache1, cache2,
												cache3,
												(String) buffer0.getBuffer()
														.elementAt(0)
														.elementAt(0), ll);
									} else {
										hideLatency0 = 0;
									}
									// }

								}

								// System.out.println(cache0.getCacheLine().elementAt(localLine-1).getIndex());
							} else if (aa[1].equals("W")) {
								// P0W++;
								/** 
								 * when deal with write command, it needs to check whether there is enough 
								 * room for new line, if yes, add the new line.
								 */
								if (buffer0.getBuffer().size() < bufferSize) {
									Vector<String> temp = new Vector<String>();
									temp.add(tag);
									temp.add(index);
									buffer0.getBuffer().add(temp);
									
									/**
									 * if the size of buffer is large enough to trigger
									 * retire policy, then do the retire policy
									 */
									
									if (buffer0.getBuffer().size() >= retire
											&& hideLatency0 == 0) {
										// sign=1;
										hideLatency0 = latency0
												+ busTransaction + accessMemory
												+ cacheHit;
										BigInteger temp1 = new BigInteger(
												(String) buffer0.getBuffer()
														.elementAt(0)
														.elementAt(1), 2);
										int ll = Integer.parseInt(temp1
												.toString());
										Cache0WriteMiss += method.write(cache0,
												(String) buffer0.getBuffer()
														.elementAt(0)
														.elementAt(0),
												(String) buffer0.getBuffer()
														.elementAt(0)
														.elementAt(1), "0", ll);
										method.checkInvalid(cache1, cache2,
												cache3,
												(String) buffer0.getBuffer()
														.elementAt(0)
														.elementAt(0), ll);
										// method.setMemory(mem, tag, index,
										// aa[0], aa[1]);
									}
								}
								
								/**
								 * if the buffer is full, then release the buffer
								 */
								if (buffer0.getBuffer().size() == bufferSize) {

									while (buffer0.getBuffer().size() != 0) {
										BigInteger temp1 = new BigInteger(
												(String) buffer0.getBuffer()
														.elementAt(0)
														.elementAt(1), 2);
										int ll = Integer.parseInt(temp1
												.toString());
										Cache0WriteMiss += method.write(cache0,
												(String) buffer0.getBuffer()
														.elementAt(0)
														.elementAt(0),
												(String) buffer0.getBuffer()
														.elementAt(0)
														.elementAt(1), "0", ll);
										method.checkInvalid(cache1, cache2,
												cache3,
												(String) buffer0.getBuffer()
														.elementAt(0)
														.elementAt(0), ll);
										buffer0.getBuffer().removeElementAt(0);
										latency0 = latency0 + busTransaction
												+ accessMemory + cacheHit;
										hideLatency0 = 0;
									}

								}

							}
							//System.out.println(latency0);
						}
						if (aa[0].equals("P1")) {
							// countP1++;
							if (aa[1].equals("R")) {
								// P0R++;
								int flag = 0;
								if (buffer1.getBuffer().size() != 0) {
									for (int i = 0; i < buffer1.getBuffer()
											.size(); i++) {
										if ((tag + index).equals(buffer1
												.getBuffer().elementAt(i))) {
											latency1 = latency1 + readBypass;
											flag = 1;
										}
									}
								}

								if (flag == 0) {
									if (cache1.getCacheLine()
											.elementAt(localLine).getTag()
											.equals(tag)
											&& !cache1.getCacheLine()
													.elementAt(localLine)
													.getStatus()
													.equals("Invalid")) {
										// System.out.println("Coherence missssssssssssssssss");
										// P0CoherenceMiss++;
										latency1 = latency1 + cacheHit;
									} else {
										if (method.checkRead(cache0, cache2,
												cache3, tag, localLine) == true) {
											latency1 = latency1
													+ busTransaction + cacheHit;
										} else {
											latency1 = latency1
													+ busTransaction
													+ accessMemory + cacheHit;
										}
									}

									method.read(cache1, tag, index, offset,
											localLine);
									method.checkModified(cache0, cache2,
											cache3, tag, localLine);
									method.setMemory(mem, tag, index, aa[0],
											aa[1]);
								}

								flag = 0;

								if (latency1 >= hideLatency1
										&& hideLatency1 != 0) {
									// if(buffer1.getBuffer().size()!=0){
									buffer1.getBuffer().removeElementAt(0);
									// sign=0;
									if (buffer1.getBuffer().size() >= retire) {
										// sign=1;
										hideLatency1 = latency1
												+ busTransaction + accessMemory
												+ cacheHit;
										BigInteger temp1 = new BigInteger(
												(String) buffer1.getBuffer()
														.elementAt(0)
														.elementAt(1), 2);
										int ll = Integer.parseInt(temp1
												.toString());
										Cache1WriteMiss += method.write(cache1,
												(String) buffer1.getBuffer()
														.elementAt(0)
														.elementAt(0),
												(String) buffer1.getBuffer()
														.elementAt(0)
														.elementAt(1), "0", ll);
										method.checkInvalid(cache1, cache2,
												cache3,
												(String) buffer1.getBuffer()
														.elementAt(0)
														.elementAt(0), ll);
									} else {
										hideLatency1 = 0;
									}
									// }

								}

								// System.out.println(.getCacheLine().elementAt(localLine-1).getIndex());
							} else if (aa[1].equals("W")) {
								// P0W++;

								if (buffer1.getBuffer().size() < bufferSize) {
									Vector<String> temp = new Vector<String>();
									temp.add(tag);
									temp.add(index);
									buffer1.getBuffer().add(temp);
									if (buffer1.getBuffer().size() >= retire
											&& hideLatency1 == 0) {
										// sign=1;
										hideLatency1 = latency1
												+ busTransaction + accessMemory
												+ cacheHit;
										BigInteger temp1 = new BigInteger(
												(String) buffer1.getBuffer()
														.elementAt(0)
														.elementAt(1), 2);
										int ll = Integer.parseInt(temp1
												.toString());
										Cache0WriteMiss += method.write(cache1,
												(String) buffer1.getBuffer()
														.elementAt(0)
														.elementAt(0),
												(String) buffer1.getBuffer()
														.elementAt(0)
														.elementAt(1), "0", ll);
										method.checkInvalid(cache0, cache2,
												cache3,
												(String) buffer1.getBuffer()
														.elementAt(0)
														.elementAt(0), ll);
										// method.setMemory(mem, tag, index,
										// aa[0], aa[1]);
									}
								}
								if (buffer1.getBuffer().size() == bufferSize) {

									while (buffer1.getBuffer().size() != 0) {
										BigInteger temp1 = new BigInteger(
												(String) buffer1.getBuffer()
														.elementAt(0)
														.elementAt(1), 2);
										int ll = Integer.parseInt(temp1
												.toString());
										Cache0WriteMiss += method.write(cache1,
												(String) buffer1.getBuffer()
														.elementAt(0)
														.elementAt(0),
												(String) buffer1.getBuffer()
														.elementAt(0)
														.elementAt(1), "0", ll);
										method.checkInvalid(cache0, cache2,
												cache3,
												(String) buffer1.getBuffer()
														.elementAt(0)
														.elementAt(0), ll);
										buffer1.getBuffer().removeElementAt(0);
										latency1 = latency1 + busTransaction
												+ accessMemory + cacheHit;
										hideLatency1 = 0;
									}

								}

							}

						}
						if (aa[0].equals("P2")) {
							if (aa[1].equals("R")) {
								// P0R++;
								int flag = 0;
								if (buffer2.getBuffer().size() != 0) {
									for (int i = 0; i < buffer2.getBuffer()
											.size(); i++) {
										if ((tag + index).equals(buffer2
												.getBuffer().elementAt(i))) {
											latency2 = latency2 + readBypass;
											flag = 1;
										}
									}
								}

								if (flag == 0) {
									if (cache2.getCacheLine()
											.elementAt(localLine).getTag()
											.equals(tag)
											&& !cache2.getCacheLine()
													.elementAt(localLine)
													.getStatus()
													.equals("Invalid")) {
										// System.out.println("Coherence missssssssssssssssss");
										// P0CoherenceMiss++;
										latency2 = latency2 + cacheHit;
									} else {
										if (method.checkRead(cache1, cache2,
												cache3, tag, localLine) == true) {
											latency2 = latency2
													+ busTransaction + cacheHit;
										} else {
											latency2 = latency2
													+ busTransaction
													+ accessMemory + cacheHit;
										}
									}

									method.read(cache2, tag, index, offset,
											localLine);
									method.checkModified(cache1, cache0,
											cache3, tag, localLine);
									method.setMemory(mem, tag, index, aa[0],
											aa[1]);
								}

								flag = 0;

								if (latency2 >= hideLatency2
										&& hideLatency2 != 0) {
									// if(buffer2.getBuffer().size()!=0){
									buffer2.getBuffer().removeElementAt(0);
									// sign=0;
									if (buffer2.getBuffer().size() >= retire) {
										// sign=1;
										hideLatency2 = latency2
												+ busTransaction + accessMemory
												+ cacheHit;
										BigInteger temp1 = new BigInteger(
												(String) buffer2.getBuffer()
														.elementAt(0)
														.elementAt(1), 2);
										int ll = Integer.parseInt(temp1
												.toString());
										Cache2WriteMiss += method.write(cache2,
												(String) buffer2.getBuffer()
														.elementAt(0)
														.elementAt(0),
												(String) buffer2.getBuffer()
														.elementAt(0)
														.elementAt(1), "0", ll);
										method.checkInvalid(cache1, cache0,
												cache3,
												(String) buffer2.getBuffer()
														.elementAt(0)
														.elementAt(0), ll);
									} else {
										hideLatency2 = 0;
									}
									// }

								}

								// System.out.println(cache0.getCacheLine().elementAt(localLine-1).getIndex());
							} else if (aa[1].equals("W")) {
								// P0W++;

								if (buffer2.getBuffer().size() < bufferSize) {
									Vector<String> temp = new Vector<String>();
									temp.add(tag);
									temp.add(index);
									buffer2.getBuffer().add(temp);
									if (buffer2.getBuffer().size() >= retire
											&& hideLatency2 == 0) {
										// sign=1;
										hideLatency2 = latency2
												+ busTransaction + accessMemory
												+ cacheHit;
										BigInteger temp1 = new BigInteger(
												(String) buffer2.getBuffer()
														.elementAt(0)
														.elementAt(1), 2);
										int ll = Integer.parseInt(temp1
												.toString());
										Cache0WriteMiss += method.write(cache2,
												(String) buffer2.getBuffer()
														.elementAt(0)
														.elementAt(0),
												(String) buffer2.getBuffer()
														.elementAt(0)
														.elementAt(1), "0", ll);
										method.checkInvalid(cache1, cache0,
												cache3,
												(String) buffer2.getBuffer()
														.elementAt(0)
														.elementAt(0), ll);
										// method.setMemory(mem, tag, index,
										// aa[0], aa[1]);
									}
								}
								if (buffer2.getBuffer().size() == bufferSize) {

									while (buffer2.getBuffer().size() != 0) {
										BigInteger temp1 = new BigInteger(
												(String) buffer2.getBuffer()
														.elementAt(0)
														.elementAt(1), 2);
										int ll = Integer.parseInt(temp1
												.toString());
										Cache0WriteMiss += method.write(cache2,
												(String) buffer2.getBuffer()
														.elementAt(0)
														.elementAt(0),
												(String) buffer2.getBuffer()
														.elementAt(0)
														.elementAt(1), "0", ll);
										method.checkInvalid(cache1, cache0,
												cache3,
												(String) buffer2.getBuffer()
														.elementAt(0)
														.elementAt(0), ll);
										buffer2.getBuffer().removeElementAt(0);
										latency2 = latency2 + busTransaction
												+ accessMemory + cacheHit;
										hideLatency2 = 0;
									}

								}

							}
						}

						if (aa[0].equals("P3")) {
							if (aa[1].equals("R")) {
								// P0R++;
								int flag = 0;
								if (buffer3.getBuffer().size() != 0) {
									for (int i = 0; i < buffer3.getBuffer()
											.size(); i++) {
										if ((tag + index).equals(buffer3
												.getBuffer().elementAt(i))) {
											latency3 = latency3 + readBypass;
											flag = 1;
										}
									}
								}

								if (flag == 0) {
									if (cache3.getCacheLine()
											.elementAt(localLine).getTag()
											.equals(tag)
											&& !cache3.getCacheLine()
													.elementAt(localLine)
													.getStatus()
													.equals("Invalid")) {
										// System.out.println("Coherence missssssssssssssssss");
										// P0CoherenceMiss++;
										latency3 = latency3 + cacheHit;
									} else {
										if (method.checkRead(cache1, cache2,
												cache3, tag, localLine) == true) {
											latency3 = latency3
													+ busTransaction + cacheHit;
										} else {
											latency3 = latency3
													+ busTransaction
													+ accessMemory + cacheHit;
										}
									}

									method.read(cache3, tag, index, offset,
											localLine);
									method.checkModified(cache1, cache2,
											cache0, tag, localLine);
									method.setMemory(mem, tag, index, aa[0],
											aa[1]);
								}

								flag = 0;

								if (latency3 >= hideLatency3
										&& hideLatency3 != 0) {
									// if(buffer3.getBuffer().size()!=0){
									buffer3.getBuffer().removeElementAt(0);
									// sign=0;
									if (buffer3.getBuffer().size() >= retire) {
										// sign=1;
										hideLatency3 = latency3
												+ busTransaction + accessMemory
												+ cacheHit;
										BigInteger temp1 = new BigInteger(
												(String) buffer3.getBuffer()
														.elementAt(0)
														.elementAt(1), 2);
										int ll = Integer.parseInt(temp1
												.toString());
										Cache0WriteMiss += method.write(cache3,
												(String) buffer3.getBuffer()
														.elementAt(0)
														.elementAt(0),
												(String) buffer3.getBuffer()
														.elementAt(0)
														.elementAt(1), "0", ll);
										method.checkInvalid(cache1, cache2,
												cache0,
												(String) buffer3.getBuffer()
														.elementAt(0)
														.elementAt(0), ll);
									} else {
										hideLatency3 = 0;
									}
									// }

								}

								// System.out.println(cache0.getCacheLine().elementAt(localLine-1).getIndex());
							} else if (aa[1].equals("W")) {
								// P0W++;

								if (buffer3.getBuffer().size() < bufferSize) {
									Vector<String> temp = new Vector<String>();
									temp.add(tag);
									temp.add(index);
									buffer3.getBuffer().add(temp);
									if (buffer3.getBuffer().size() >= retire
											&& hideLatency3 == 0) {
										// sign=1;
										hideLatency3 = latency3
												+ busTransaction + accessMemory
												+ cacheHit;
										BigInteger temp1 = new BigInteger(
												(String) buffer3.getBuffer()
														.elementAt(0)
														.elementAt(1), 2);
										int ll = Integer.parseInt(temp1
												.toString());
										Cache0WriteMiss += method.write(cache3,
												(String) buffer3.getBuffer()
														.elementAt(0)
														.elementAt(0),
												(String) buffer3.getBuffer()
														.elementAt(0)
														.elementAt(1), "0", ll);
										method.checkInvalid(cache1, cache2,
												cache0,
												(String) buffer3.getBuffer()
														.elementAt(0)
														.elementAt(0), ll);
										// method.setMemory(mem, tag, index,
										// aa[0], aa[1]);
									}
								}
								if (buffer3.getBuffer().size() == bufferSize) {

									while (buffer3.getBuffer().size() != 0) {
										BigInteger temp1 = new BigInteger(
												(String) buffer3.getBuffer()
														.elementAt(0)
														.elementAt(1), 2);
										int ll = Integer.parseInt(temp1
												.toString());
										Cache0WriteMiss += method.write(cache3,
												(String) buffer3.getBuffer()
														.elementAt(0)
														.elementAt(0),
												(String) buffer3.getBuffer()
														.elementAt(0)
														.elementAt(1), "0", ll);
										method.checkInvalid(cache1, cache2,
												cache0,
												(String) buffer3.getBuffer()
														.elementAt(0)
														.elementAt(0), ll);
										buffer3.getBuffer().removeElementAt(0);
										latency3 = latency3 + busTransaction
												+ accessMemory + cacheHit;
										hideLatency3 = 0;
									}

								}

							}

						}
						count++;
					}
					reader.close();
					read.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			} catch (UnsupportedEncodingException e) {
				e.printStackTrace();
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}

		}
		
//		latency0=latency0+ (buffer0.getBuffer().size()*222);
//		latency1=latency1+ (buffer1.getBuffer().size()*222);
//		latency2=latency2+ (buffer2.getBuffer().size()*222);
//		latency3=latency3+ (buffer3.getBuffer().size()*222);
		

//		System.out.println(latency0 + " " + latency1 + " " + latency2 + " "
//				+ latency3);
		System.out.println("latency of p0 is: "+latency0);
		System.out.println("latency of p1 is: "+latency1);
		System.out.println("latency of p2 is: "+latency2);
		System.out.println("latency of p3 is: "+latency3);
		
		
		//System.out.println("bufferSize for P0 is: "+buffer0.getBuffer().size());

		/** to calculate the statistic in the experiment */
		int privateLines = 0;
		int shareRead = 0;
		int shareReadWrite = 0;
		int numberOfOne = 0;
		int numberOfTwo = 0;
		int numberOfMul = 0;
		for (int m = 0; m < mem.getLines().size(); m++) {
			if (mem.getLines().elementAt(m).getProcessor().size() == 1) {
				privateLines = privateLines
						+ mem.getLines().elementAt(m).getVisitTimes();
				numberOfOne++;

				// System.out.println(mem.getLines().elementAt(m).getVisitTimes());
				// System.out.println("ddddddddd");
			} else {
				if (mem.getLines().elementAt(m).getProcessor().size() == 2) {
					numberOfTwo++;
				} else {
					numberOfMul++;
				}
				if (mem.getLines().elementAt(m).getRead() == true
						&& mem.getLines().elementAt(m).getWrite() == false) {
					shareRead = shareRead
							+ mem.getLines().elementAt(m).getVisitTimes();
				}
				if (mem.getLines().elementAt(m).getRead() == true
						|| mem.getLines().elementAt(m).getWrite() == true) {
					shareReadWrite = shareReadWrite
							+ mem.getLines().elementAt(m).getVisitTimes();
				}
			}
		}

		java.text.NumberFormat nf = java.text.NumberFormat.getPercentInstance();

		nf.setMinimumFractionDigits(2);

		// String PrivateLinesRate = nf.format((double)privateLines/count);
		// String SharedLineRate = nf.format((double)shareRead/count);
		// String SharedRWRate = nf.format((double)shareReadWrite/count);
		// String oneProcessor =
		// nf.format((double)numberOfOne/mem.getLines().size());
		// String twoProcessor =
		// nf.format((double)numberOfTwo/mem.getLines().size());
		// String MulProcessor =
		// nf.format((double)numberOfMul/mem.getLines().size());

		/*
		 * the information of each cache after total running...
		 */

		// for (int p=0; p< cache0.getCacheLine().size(); p++){
		// System.out.println(p+" "+cache0.getCacheLine().elementAt(p).getTag()+cache0.getCacheLine().elementAt(p).getIndex()
		// +cache0.getCacheLine().elementAt(p).getStatus());
		// }
		//
		// for (int p=0; p< cache1.getCacheLine().size(); p++){
		// System.out.println(p+" "+cache1.getCacheLine().elementAt(p).getTag()+cache1.getCacheLine().elementAt(p).getIndex()
		// +cache1.getCacheLine().elementAt(p).getStatus());
		// }
		// for (int p=0; p< cache2.getCacheLine().size(); p++){
		// System.out.println(p+" "+cache2.getCacheLine().elementAt(p).getTag()+cache2.getCacheLine().elementAt(p).getIndex()
		// +cache2.getCacheLine().elementAt(p).getStatus());
		// }
		// for (int p=0; p< cache3.getCacheLine().size(); p++){
		// System.out.println(p+" "+cache3.getCacheLine().elementAt(p).getTag()+cache3.getCacheLine().elementAt(p).getIndex()
		// +cache3.getCacheLine().elementAt(p).getStatus());
		// }

		// String P0readMiss = nf.format((double)Cache0ReadMiss/P0R);
		// String P0writeMiss = nf.format((double)Cache0WriteMiss/P0W);
		// String P0totalMiss =
		// nf.format((double)(Cache0ReadMiss+Cache0WriteMiss)/countP0);
		// String P0CoherenceMissRate =
		// nf.format((double)P0CoherenceMiss/(Cache0ReadMiss+Cache0WriteMiss));
		//
		//
		//
		// System.out.println("Miss rate for P0 is: "+ P0totalMiss);
		// System.out.println("Read miss rate for P0 is: "+P0readMiss);
		// System.out.println("Write miss rate for P0 is: "+P0writeMiss);
		// System.out.println("Coherence miss rate for P0 is: "+
		// P0CoherenceMissRate);
		// System.out.println("************************************************************");
		//
		// String P1readMiss = nf.format((double)Cache1ReadMiss/P1R);
		// String P1writeMiss = nf.format((double)Cache1WriteMiss/P1W);
		// String P1totalMiss =
		// nf.format((double)(Cache1ReadMiss+Cache1WriteMiss)/countP1);
		// String P1CoherenceMissRate =
		// nf.format((double)P1CoherenceMiss/(Cache1ReadMiss+Cache1WriteMiss));
		// //System.out.println("************************************************************");
		//
		//
		//
		// System.out.println("Miss rate for P1 is: "+ P1totalMiss);
		// System.out.println("Read miss rate for P1 is: "+P1readMiss);
		// System.out.println("Write miss rate for P1 is: "+P1writeMiss);
		// System.out.println("Coherence miss rate for P1 is: "+
		// P1CoherenceMissRate);
		// System.out.println("************************************************************");
		//
		//
		// String P2readMiss = nf.format((double)Cache2ReadMiss/P2R);
		// String P2writeMiss = nf.format((double)Cache2WriteMiss/P2W);
		// String P2totalMiss =
		// nf.format((double)(Cache2ReadMiss+Cache2WriteMiss)/countP2);
		// String P2CoherenceMissRate =
		// nf.format((double)P2CoherenceMiss/(Cache2ReadMiss+Cache2WriteMiss));
		//
		//
		// System.out.println("Miss rate for P2 is: "+ P2totalMiss);
		// System.out.println("Read miss rate for P2 is: "+P2readMiss);
		// System.out.println("Write miss rate for P2 is: "+P2writeMiss);
		// System.out.println("Coherence miss rate for P2 is: "+
		// P2CoherenceMissRate);
		// System.out.println("************************************************************");
		//
		//
		// String P3readMiss = nf.format((double)Cache3ReadMiss/P3R);
		// String P3writeMiss = nf.format((double)Cache3WriteMiss/P3W);
		// String P3totalMiss =
		// nf.format((double)(Cache3ReadMiss+Cache3WriteMiss)/countP3);
		// String P3CoherenceMissRate =
		// nf.format((double)P3CoherenceMiss/(Cache3ReadMiss+Cache3WriteMiss));
		//
		//
		//
		// System.out.println("Miss rate for P3 is: "+ P3totalMiss);
		// System.out.println("Read miss rate for P3 is: "+P3readMiss);
		// System.out.println("Write miss rate for P3 is: "+P3writeMiss);
		// System.out.println("Coherence miss rate for P3 is: "+
		// P3CoherenceMissRate);
		// System.out.println("************************************************************");
		//
		//
		// System.out.println("Percentage of memory accesses to private cache-lines is: "+
		// PrivateLinesRate);
		// System.out.println("Percentage of memory accesses to Shared read-only cache-line is: "+
		// SharedLineRate);
		// System.out.println("Percentage of memory accesses to Shared read-write cache-lines is: "+
		// SharedRWRate);
		// System.out.println("Percentage of memory addresses that are accessed by one processor is: "+
		// oneProcessor);
		// System.out.println("Percentage of memory addresses that are accessed by two processor is: "+
		// twoProcessor);
		// System.out.println("Percentage of memory addresses that are accessed by Multiple processor is: "+
		// MulProcessor);

	}
}
