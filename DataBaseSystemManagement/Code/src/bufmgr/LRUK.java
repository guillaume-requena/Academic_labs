/* File LRUK.java */

package bufmgr;

import diskmgr.*;
import global.*;

  /**
   * class LRUK is a subclass of class Replacer using LRUK
   * algorithm for page replacement
   */
public class LRUK extends  Replacer {

  /**
   * private field
   * An array to hold number of frames in the buffer pool
   */

    private int  frames[];
    private int lastRef ;
    private long LAST[] ;  
    private long HIST[][] ; 
   
 
  /**
   * private field
   * number of frames used
   */   
  private int  nframes;

  /**
   * This pushes the given frame to the end of the list.
   * @param frameNo	the frame number
   */
  private void update(int frameNo)
  {

	 long t = System.currentTimeMillis();
	 for ( int i = 1; i < lastRef ; i++) 
	     HIST[frameNo][i] = HIST[frameNo][i-1]  ;

	HIST[frameNo][0] = t;
    	LAST[frameNo] = t;
		
  }

  /**
   * Calling super class the same method
   * Initializing the frames[] with number of buffer allocated
   * by buffer manager
   * set number of frame used to zero
   *
   * @param	mgr	a BufMgr object
   * @see	BufMgr
   * @see	Replacer
   */
    public void setBufferManager( BufMgr mgr )
     {
        super.setBufferManager(mgr);
	frames = new int [ mgr.getNumBuffers() ];
	nframes = 0;
	
     }

/* public methods */

  /**
   * Class constructor
   * Initializing frames[] pinter = null.
   */
    public LRUK(BufMgr mgrArg, int lastRef)
    {
      super(mgrArg);
      frames = null;
      this.lastRef = lastRef ;
      LAST = new long [ mgrArg.getNumBuffers() ] ; //the default values of time are 0
      HIST = new long[ mgrArg.getNumBuffers() ][lastRef] ; 
      

    }
  
  /**
   * call super class the same method
   * pin the page in the given frame number 
   * move the page to the end of list  
   *
   * @param	 frameNo	 the frame number to pin
   * @exception  InvalidFrameNumberException
   */
 public void pin(int frameNo) throws InvalidFrameNumberException
 {
    super.pin(frameNo);

    update(frameNo);
    
 }

  /**
   * Finding a free frame in the buffer pool
   * or choosing a page to replace using LRUK policy
   * update(frame);
   * @return 	return the frame number
   *		return -1 if failed
   */

 public int pick_victim() throws BufferPoolExceededException
 {
   long t = System.currentTimeMillis();
   int numBuffers = mgr.getNumBuffers();
   int frame;
   int victim = -1; 
   int imin = 0; //use it to store the index of the victim
   
    if ( nframes < numBuffers) {
        frame = nframes++;
        frames[frame] = frame;
        state_bit[frame].state = Pinned;
        (mgr.frameTable())[frame].pin();
        return frame;
    }
    
    else {
  //in the case where there is no available frame in the buffer
  //we look at the "Last K-Recently Used" unpinned pages
    long min = System.currentTimeMillis();
    
    for ( int i = 0; i < numBuffers; ++i ) {
    	frame=frames[i];
        if(( state_bit[frame].state != Pinned ) && (HIST[i][lastRef-1]<=min)) {
        	victim = frame;
        	min = HIST[i][lastRef-1];
        	imin = i;
        }
    }
    }
    
    if (victim != -1) { //it means we just found a victim
    	state_bit[victim].state = Pinned; //we update the state bit and the frame
        (mgr.frameTable())[victim].pin();
        
        for ( int j = 1; j < lastRef; ++j) {
        	HIST[imin][j] = 0; //reinitialize the corresponding line in hist
        }
        HIST[imin][0]=t;
        
        return victim;
   
    }
    throw new BufferPoolExceededException (null,"BUFMGR: BUFFER_EXCEEDED."); //in case of all pages in the buffer are marked as pin
 }
  /**
   * get the page replacement policy name
   *
   * @return	return the name of replacement policy used
   */  
    public String name() { return "LRUK"; }
 
  /**
   * print out the information of frame usage
   */  
 public void info()
 {
    super.info();

    System.out.print( "LRUK REPLACEMENT");
    
    for (int i = 0; i < nframes; i++) {
        if (i % 5 == 0)
	System.out.println( );
	System.out.print( "\t" + frames[i]);
        
    }
    System.out.println();
 }
 
 public long HIST(int pagenumber,int i) {  // method used in test4() method in BMTest2.java
	 
	 return HIST[pagenumber][i];
 }
 
 public int[] getFrames() {  // method used in test4() method in BMTest2.java
		return frames;
	}
 
public long last(int pagenumber) {  // method used in test4() method in BMTest2.java
	 
	 return LAST[pagenumber];
 }
  
}



