package iterator;


import heap.*;

import global.*;
import bufmgr.*;
import diskmgr.*;
import index.*;
import java.lang.*;
import java.io.*;
import java.util.*;
/**
*
*  This file contains an implementation of the Self Inequality Join for one predicate
*  algorithm as described in the VLDBJ paper.
*
*/

public class IEJoin2c  extends Iterator
{
  private AttrType      _in1[], _in2[];
  private   int        in1_len, in2_len;
  private   Iterator  outer;
  private   Iterator  outer2;
  private   Iterator  inner;
  private   Iterator  inner2;
  private   short t1_str_sizescopy[];
  private   short t2_str_sizescopy[];
  private   CondExpr OutputFilter[];
  private   CondExpr RightFilter[];
  private   int        n_buf_pgs;        // # of buffer pages available.
  private   boolean        done,         // Is the join complete
  get_from_outer;                 // if TRUE, a tuple is got from outer
  private   Tuple     outer_tuple, inner_tuple;
  private   Tuple     Jtuple;           // Joined tuple
  private   FldSpec   perm_mat[];
  private   int        nOutFlds;
  private int eqOff;
  private ArrayList<Tuple> Join_Result;
  private int[] P;
  private int[] B;
  private int[] Pp;
  private int[] O2;
  private int[] O1;


  /**constructor
  *Initialize the relations which is self joined, including relation type,
  *@param in1  Array containing field types of q.
  *@param len_in1  # of columns in q.
  *@param t1_str_sizes shows the length of the string fields.
  *@param amt_of_mem  IN PAGES
  *@param am1  access method for left i/p to join
  *@param relationName  access hfapfile for right i/p to join
  *@param outFilter   select expressions
  *@param rightFilter reference to filter applied on right i/p
  *@param proj_list shows what input fields go where in the output tuple
  *@param n_out_flds number of outer relation fields
  *@exception IOException some I/O fault
  *@exception NestedLoopException exception from this class
 * @throws SortException 
  */
  public IEJoin2c(
		  AttrType    in1[],    
		   int     len_in1,           
		   short   t1_str_sizes[],
		   AttrType    in2[],         
		   int     len_in2,           
		   short   t2_str_sizes[],   
  int     amt_of_mem,
  Iterator     am1,
  Iterator     am2,
  Iterator     am3,
  Iterator     am4,
  CondExpr outFilter[],
  FldSpec   proj_list[],
  int        n_out_flds
  ) throws IOException,NestedLoopException, SortException
  {

    _in1 = new AttrType[in1.length];
    _in2 = new AttrType[in2.length];
    System.arraycopy(in1,0,_in1,0,in1.length);
    System.arraycopy(in2,0,_in2,0,in2.length);
    in1_len = len_in1;
    in2_len = len_in2;


    outer = am1;
    outer2 = am2;
    
    inner = am3;
    inner2 = am4;
    
    t1_str_sizescopy =  t1_str_sizes;
    t2_str_sizescopy =  t2_str_sizes;
    inner_tuple = new Tuple();
    Jtuple = new Tuple();
    OutputFilter = outFilter;

    n_buf_pgs    = amt_of_mem;
    done  = false;
    get_from_outer = true;

    AttrType[] Jtypes = new AttrType[n_out_flds];
    short[]    t_size;


    perm_mat = proj_list;
    nOutFlds = n_out_flds;
    try {
      t_size = TupleUtils.setup_op_tuple(Jtuple, Jtypes,
      in1, len_in1, in1, len_in1,
      t1_str_sizes, t1_str_sizes,
      proj_list, nOutFlds);
    }
    catch (TupleUtilsException e){
      throw new NestedLoopException(e,"TupleUtilsException is caught by IEJoin2c.java");
    }
    
    //Here we construct the IEjoin algorithm
    ArrayList<Tuple> L1 = new ArrayList<Tuple>() ; // it will contain all the tuple of our single table
    ArrayList<Tuple> L2 = new ArrayList<Tuple>() ;
    ArrayList<Tuple> L1p = new ArrayList<Tuple>() ;
    ArrayList<Tuple> L2p = new ArrayList<Tuple>() ;

    Sort sorted_tableL1 = null;
    Sort sorted_tableL2 = null;
    Sort sorted_tableL1p = null;
    Sort sorted_tableL2p = null;
    
    if (outFilter[0].op.toString() == "aopGT" || outFilter[0].op.toString() == "aopGE")//sort L1 in ascending order
    {
      TupleOrder ascending = new TupleOrder(TupleOrder.Ascending); //using the TupleOrder.java from the global folder
      try {
        sorted_tableL1 = new Sort (in1,(short)in1_len, t1_str_sizes,
        (iterator.Iterator) outer, outFilter[0].operand1.symbol.offset, ascending, t1_str_sizes[0], 10);
        sorted_tableL1p = new Sort (in2,(short)in2_len, t2_str_sizes,
        (iterator.Iterator) inner, outFilter[0].operand2.symbol.offset, ascending, t2_str_sizes[0], 10);
      }
      catch(Exception e)
      {
        throw new SortException (e, "Sort failed");
      }
    }
    else if (outFilter[0].op.toString() == "aopLT" || outFilter[0].op.toString() == "aopLE")//sort L1 in descending order
    {
      TupleOrder descending = new TupleOrder(TupleOrder.Descending);
      try {
        sorted_tableL1 = new Sort(in1,(short)in1_len, t1_str_sizes,
        (iterator.Iterator) outer, outFilter[0].operand1.symbol.offset, descending, t1_str_sizes[0], 10);
        sorted_tableL1p = new Sort(in2,(short)in2_len, t2_str_sizes,
        (iterator.Iterator) inner, outFilter[0].operand2.symbol.offset, descending, t2_str_sizes[0], 10);
      }
      catch(Exception e)
      {
        throw new SortException (e, "Sort failed");
      }
    }
    
    if (outFilter[1].op.toString() == "aopGT" || outFilter[1].op.toString() == "aopGE")//sort L2 in descending order
    {
      TupleOrder descending = new TupleOrder(TupleOrder.Descending); //using the TupleOrder.java from the global folder
      try {
        sorted_tableL2 = new Sort (in1,(short)in1_len, t1_str_sizes,
        (iterator.Iterator) outer2, outFilter[1].operand1.symbol.offset, descending, t1_str_sizes[0], 10);
        sorted_tableL2p = new Sort (in2,(short)in2_len, t2_str_sizes,
        (iterator.Iterator) inner2, outFilter[1].operand2.symbol.offset, descending, t2_str_sizes[0], 10);
      }
      catch(Exception e)
      {
        throw new SortException (e, "Sort failed");
      }
    }
    else if (outFilter[1].op.toString() == "aopLT" || outFilter[1].op.toString() == "aopLE")//sort L2 in ascending order
    {
      TupleOrder ascending = new TupleOrder(TupleOrder.Ascending);
      try {
        sorted_tableL2 = new Sort(in1,(short)in1_len, t1_str_sizes,
        (iterator.Iterator) outer2, outFilter[1].operand1.symbol.offset, ascending, t1_str_sizes[0], 10);
        sorted_tableL2p = new Sort(in2,(short)in1_len, t2_str_sizes,
        (iterator.Iterator) inner2, outFilter[1].operand2.symbol.offset, ascending, t2_str_sizes[0], 10);
      }
      catch(Exception e)
      {
        throw new SortException (e, "Sort failed");
      }
    }
    
    // equality offset variable
    if ((outFilter[0].op.toString() == "aopGE" || outFilter[0].op.toString() == "aopLE") && (outFilter[1].op.toString() == "aopGE" || outFilter[1].op.toString() == "aopLE")){
      eqOff = 0;
    }
    else {
      eqOff = 1;
    }

    int nL1 = 0;
    int nL2 = 0;
    int nL1p = 0;
    int nL2p = 0;
    Tuple t;
    try {
    while ((t = sorted_tableL1.get_next()) != null) {
      nL1 = nL1 + 1;
      Tuple x = new Tuple(t);
      L1.add(x);
    }
    }catch(Exception e) {
    	System.out.println(e);
    }

    try {
    while ((t = sorted_tableL2.get_next()) != null) {
      nL2 = nL2 + 1;
      Tuple x = new Tuple(t);
      L2.add(x);
    }
    }catch(Exception e) {
    	System.out.println(e);
    }
    
    try {
    while ((t = sorted_tableL1p.get_next()) != null) {
      Tuple x = new Tuple(t);
      nL1p = nL1p + 1;
      L1p.add(x);
    }
    }catch(Exception e) {
    	System.out.println(e);
    }

    try {
    while ((t = sorted_tableL2p.get_next()) != null) {
      Tuple x = new Tuple(t);
      nL2p = nL2p + 1;
      L2p.add(x);
    }
    }catch(Exception e) {
    	System.out.println(e);
    }
    
    
    //Now we can create the list of the pair tuples that check
    //the condition of the query by using the loop in the paper
    //but without the need of permutation and bit array
    
    P = new int[nL1];
    B = new int[nL1];
    try {
    for (int i = 0; i < nL1; i++) {
    	Tuple tupleA = L1.get(i);
    	for (int j = 0; j < nL2; j++){
    		Tuple tupleB = L2.get(j);
    		for (int k = 1; k <= in1_len; k++) {
    			if (tupleA.getIntFld(k) == tupleB.getIntFld(k)) {
    				if (k == in1_len) {
    					P[j] = i;
    				}
    			}
    		}
    	}
    }
    } catch (Exception e) {
    	System.out.println(e);
    }
    
    
    Pp = new int[nL1];
    try {
    for (int i = 0; i < nL1; i++) {
    	Tuple tupleA = L1p.get(i);
    	for (int j = 0; j < nL2; j++){
    		Tuple tupleB = L2p.get(j);
    		for (int k = 1; k <= in1_len; k++) {
    			if (tupleA.getIntFld(k) == tupleB.getIntFld(k)) {
    				if (k == in1_len) {
    					Pp[j] = i;
    				}
    			}
    		}
    	}
    }
    } catch (Exception e) {
    	System.out.println(e);
    }
    
  
    O1 = new int[nL1p];
    try {
    for (int i = 0; i < nL1p; i++){
		Tuple tuple1 = L1.get(i);
    	for (int j = 0; j < nL2p; j++){
    		Tuple tuple1p = L1p.get(j);
    	    if ((outFilter[0].op.toString() == "aopGT" || outFilter[0].op.toString() == "aopGE") && tuple1.getIntFld(outFilter[0].operand1.symbol.offset) == tuple1p.getIntFld(outFilter[0].operand2.symbol.offset)) // descending order
    	    {
    	    	O1[i] = j;
    	    	break;
    	    }
    	    else if ((outFilter[0].op.toString() == "aopGT" || outFilter[0].op.toString() == "aopGE") && tuple1.getIntFld(outFilter[0].operand1.symbol.offset) > tuple1p.getIntFld(outFilter[0].operand2.symbol.offset)) {
    	    	O1[i] = j;
    	    	break;
    	    }
    	    else if ((outFilter[0].op.toString() == "aopLT" || outFilter[0].op.toString() == "aopLE") && tuple1.getIntFld(outFilter[1].operand1.symbol.offset) < tuple1p.getIntFld(outFilter[0].operand2.symbol.offset)) {
    	    	O2[i] = j;
    	    	break;
    	    }
    	}
    }
    
    O2 = new int[nL2p];
    for (int i = 0; i < nL1; i++){
		Tuple tuple2 = L2.get(i);
    	for (int j = 0; j < nL2; j++){
    		Tuple tuple2p = L2p.get(j);   
    	    if (tuple2.getIntFld(outFilter[1].operand1.symbol.offset) == tuple2p.getIntFld(outFilter[1].operand2.symbol.offset)) // descending order
    	    {
    	    	O2[i] = j;
    	    	break;
    	    }
    	    else if ((outFilter[1].op.toString() == "aopGT" || outFilter[1].op.toString() == "aopGE") && tuple2.getIntFld(outFilter[1].operand1.symbol.offset) > tuple2p.getIntFld(outFilter[1].operand2.symbol.offset)) {
    	    	O2[i] = j;
    	    	break;
    	    }
    	    else if ((outFilter[1].op.toString() == "aopLT" || outFilter[1].op.toString() == "aopLE") && tuple2.getIntFld(outFilter[1].operand1.symbol.offset) < tuple2p.getIntFld(outFilter[1].operand2.symbol.offset)) {
    	    	O2[i] = j;
    	    	break;
    	    }
    	}
    }
    } catch(Exception e) {
    	System.out.println(e);
    }
    
    
    
    int off2 = 0;
    int off1 = 0;
    Join_Result = new ArrayList<Tuple>();
    for (int i = 0; i < nL1; i++){
    	off2 = O2[i];
    	int min;
    	if (off2 < nL2p) {
    		min = off2;
    	}
    	else {
    		min = nL2p;
    	}
    	for (int j = 0; j < min; j++){
    		B[Pp[j]] = 1;
    	}
    	off1 = O1[P[i]];
    	for (int k = off1 + eqOff; k < nL1p; k++){
    		if (B[k] == 1) {
    			Tuple tuple1 = L2.get(i);
        		Tuple tuple2 = L1p.get(k);
        		try{
        			Projection.Join(tuple1, _in1, tuple2, _in1, Jtuple, perm_mat, nOutFlds);
        			Tuple x = new Tuple(Jtuple);
        			Join_Result.add(x);
        		}
        		catch(Exception e) {System.out.println("Error ocurred when adding the result tuples");}
    			}
    		}
    		
    }
  }


  /**
  *@return The joined tuple is returned
  *@exception IOException I/O errors
  *@exception JoinsException some join exception
  *@exception IndexException exception from super class
  *@exception InvalidTupleSizeException invalid tuple size
  *@exception InvalidTypeException tuple type not valid
  *@exception PageNotReadException exception from lower layer
  *@exception TupleUtilsException exception from using tuple utilities
  *@exception PredEvalException exception from PredEval class
  *@exception SortException sort exception
  *@exception LowMemException memory error
  *@exception UnknowAttrType attribute type unknown
  *@exception UnknownKeyTypeException key type unknown
  *@exception Exception other exceptions

  */
  public Tuple get_next()
  {
Tuple t = null;
try {
	while(Join_Result.size() != 0) {
		t = Join_Result.get(0);
		Join_Result.remove(0);
		return t;
	}
}catch (Exception e) {
	System.out.println(e);
} return null;
  }

  /**
  * implement the abstract method close() from super class Iterator
  *to finish cleaning up
  *@exception IOException I/O error from lower layers
  *@exception JoinsException join error from lower layers
  *@exception IndexException index access error
  */
  public void close() throws JoinsException, IOException,IndexException
  {
    if (!closeFlag) {

      try {
        outer.close();
      }catch (Exception e) {
        throw new JoinsException(e, "IESelfJoin2b.java: error in closing iterator.");
      }
      closeFlag = true;
    }
  }
}
