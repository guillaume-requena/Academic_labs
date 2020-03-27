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

public class IESelfJoin2a  extends Iterator
{
  private AttrType      _in1[];
  private   int        in1_len;
  private   Iterator  outer;
  private   short t1_str_sizescopy[];
  private   CondExpr OutputFilter[];
  private   CondExpr RightFilter[];
  private   int        n_buf_pgs;        // # of buffer pages available.
  private   boolean        done,         // Is the join complete
  get_from_outer;                 // if TRUE, a tuple is got from outer
  private   Tuple     outer_tuple, inner_tuple;
  private   Tuple     Jtuple;           // Joined tuple
  private   FldSpec   perm_mat[];
  private   int        nOutFlds;
  private   Heapfile  hf;
  private   Scan      inner;
  private int eqOff;
  private ArrayList<Tuple> Join_Result;


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
  *@param n_out_flds number of outer relation fileds
  *@exception IOException some I/O fault
  *@exception NestedLoopException exception from this class
 * @throws SortException 
  */
  public IESelfJoin2a(
  AttrType    in1[],
  int     len_in1,
  short   t1_str_sizes[],
  int     amt_of_mem,
  Iterator     am1,
  String relationName,
  CondExpr outFilter[],
  FldSpec   proj_list[],
  int        n_out_flds
  ) throws IOException,NestedLoopException, SortException
  {

    _in1 = new AttrType[in1.length];
    System.arraycopy(in1,0,_in1,0,in1.length);
    in1_len = len_in1;


    outer = am1;
    t1_str_sizescopy =  t1_str_sizes;
    inner_tuple = new Tuple();
    Jtuple = new Tuple();
    OutputFilter = outFilter;

    n_buf_pgs    = amt_of_mem;
    inner = null;
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
      throw new NestedLoopException(e,"TupleUtilsException is caught by IESelfJoin2a.java");
    }

    //Here we construct the IEjoin algorithm

    ArrayList<Tuple> allTuples = new ArrayList<Tuple>() ; // it will contain all the tuple of our single table


    Sort sorted_table = null;
    if (outFilter[0].op.toString() == "aopGT" || outFilter[0].op.toString() == "aopGE")//sort L1 in ascending order
    {
      TupleOrder ascending = new TupleOrder(TupleOrder.Ascending); //using the TupleOrder.java from the global folder
      try {
        sorted_table = new Sort (in1,(short)in1_len, t1_str_sizes,
        (iterator.Iterator) outer, outFilter[0].operand1.symbol.offset, ascending, t1_str_sizes[0], 10);
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
        sorted_table = new Sort(in1,(short)in1_len, t1_str_sizes,
        (iterator.Iterator) outer, outFilter[0].operand1.symbol.offset, descending, t1_str_sizes[0], 10);
      }
      catch(Exception e)
      {
        throw new SortException (e, "Sort failed");
      }
    }

    // equality offset variable
    if (outFilter[0].op.toString() == "aopGE" || outFilter[0].op.toString() == "aopLE"){
      eqOff = 0;
    }
    else {
      eqOff = 1;
    }

    int n = 0;
    Tuple t;
    try {
    while ((t = sorted_table.get_next()) != null) {
      n = n + 1;
      Tuple x = new Tuple(t);
      allTuples.add(x) ;
    }
    }catch(Exception e) {
    	System.out.println(e);
    }
    

    //Now we can create the list of the pair tuples that check
    //the condition of the query by using the loop in the paper
    //but without the need of permutation and bit array

    Join_Result = new ArrayList<Tuple>();
    for (int i = 0; i < n; i++){
      Tuple tuple1 = allTuples.get(i);
      for (int j = i + eqOff; j < n; j++){
        Tuple tuple2 = allTuples.get(j);
        try{
        Projection.Join(tuple1, _in1, tuple2, _in1, Jtuple, perm_mat, nOutFlds);
          Tuple x = new Tuple(Jtuple);
          Join_Result.add(x);
        }
        catch(Exception e) {System.out.println("Error ocurred when adding the result tuples");}
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
        throw new JoinsException(e, "IESelfJoin2a.java: error in closing iterator.");
      }
      closeFlag = true;
    }
  }
}
