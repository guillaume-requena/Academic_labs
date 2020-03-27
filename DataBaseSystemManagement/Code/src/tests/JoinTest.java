package tests;


import iterator.*;
import heap.*;
import global.*;
import index.*;
import java.io.*;
import java.util.*;
import java.lang.*;
import diskmgr.*;
import bufmgr.*;
import btree.*;
import catalog.*;


class R {
  public int col1;
  public int col2;
  public int col3;
  public int col4;
  ;
  public R (int _col1, int _col2, int _col3, int _col4) {
    col1 = _col1;
    col2 = _col2;
    col3 = _col3;
    col4 = _col4;
  }
}

class S {
  public int col1;
  public int col2;
  public int col3;
  public int col4;


  public S (int _col1, int _col2, int _col3, int _col4) {
    col1 = _col1;
    col2 = _col2;
    col3 = _col3;
    col4 = _col4;
  }
}

class Q {
  public int col1;
  public int col2;
  public int col3;
  public int col4;


  public Q (int _col1, int _col2, int _col3, int _col4) {
    col1 = _col1;
    col2 = _col2;
    col3 = _col3;
    col4 = _col4;
  }
}


class JoinsDriver implements GlobalConst{

  private boolean OK = true;
  private boolean FAIL = false;
  private Vector r;
  private Vector s;
  private Vector q;
  private Vector q2;
  public Vector txt1a;
  public Vector txt1b;
  public Vector txt2a;
  public Vector txt2b;
  public Vector txt2c;
  public int numR;
  public int numS;
  public int numq;
  public int numtxt1a;
  public int numtxt1b;
  public int numtxt2a;
  public int numtxt2b;
  public int numtxt2c;

  private int selectLeft1a;
  private int selectRight1a;
  private int conditionLeft1a;
  private int conditionRight1a;
  private int op1a;


  private int selectLeft1b;
  private int selectRight1b;
  private int condition1Left1b;
  private int op11b;
  private int condition1Right1b;
  private int condition2Left1b;
  private int op21b;
  private int condition2Right1b;
  private String rac1b;

  private int selectLeft2a;
  private int selectRight2a;
  private int conditionLeft2a;
  private int conditionRight2a;
  private int op2a;

  private int selectLeft2b;
  private int selectRight2b;
  private int condition1Left2b;
  private int op12b;
  private int condition1Right2b;
  private int condition2Left2b;
  private int op22b;
  private int condition2Right2b;
  private String rac2b;


  private String nameBdd1;
  private String nameBdd2;
  private int selectLeft2c;
  private int selectRight2c;
  private int condition1Left2c;
  private int op12c;
  private int condition1Right2c;
  private int condition2Left2c;
  private int op22c;
  private int condition2Right2c;
  private String rac2c;


	public JoinsDriver() {

		r = new Vector();
		s = new Vector();
		q = new Vector();
		q2 = new Vector();

	    txt1a = new Vector();
	    txt1b = new Vector();
	    txt2a = new Vector();
	    txt2b = new Vector();
	    txt2c = new Vector();

	    numtxt2c = 0;



	    //Extract information from query_1a.txt
	    InputStream fr_txt1a;
	    try{
			fr_txt1a = new FileInputStream("../../../Output/query_1a.txt");
			InputStreamReader lct_txt1a = new InputStreamReader(fr_txt1a);
			BufferedReader br_txt1a = new BufferedReader(lct_txt1a);
			String line_txt1a;
			String[] txt1a_array;

			while ((line_txt1a = br_txt1a.readLine()) != null ) {
			    txt1a_array = line_txt1a.split(" ");
			    txt1a.addElement(txt1a_array);
			  }
			br_txt1a.close();
		}
	    catch (Exception e){
	    	System.out.println(e.toString());
	    	System.out.println("Erreur txt1a");
	    }

	    numtxt1a = txt1a.size();

	  //Extract information from query_1b.txt
	    InputStream fr_txt1b;
	    try{
			fr_txt1b = new FileInputStream("../../../Output/query_1b.txt");
			InputStreamReader lct_txt1b = new InputStreamReader(fr_txt1b);
			BufferedReader br_txt1b = new BufferedReader(lct_txt1b);
			String line_txt1b;
			String[] txt1b_array;

			while ((line_txt1b = br_txt1b.readLine()) != null ) {
			    txt1b_array = line_txt1b.split(" ");
			    txt1b.addElement(txt1b_array);
			  }
			br_txt1b.close();
		}
	    catch (Exception e){
	    	System.out.println(e.toString());
	    	System.out.println("Erreur txt1b");
	    }

	    numtxt1b = txt1b.size();

	    //Extract information from query_2a.txt
	    InputStream fr_txt2a;
	    try{
			fr_txt2a = new FileInputStream("../../../Output/query_2a.txt");
			InputStreamReader lct_txt2a = new InputStreamReader(fr_txt2a);
			BufferedReader br_txt2a = new BufferedReader(lct_txt2a);
			String line_txt2a;
			String[] txt2a_array;

			while ((line_txt2a = br_txt2a.readLine()) != null ) {
			    txt2a_array = line_txt2a.split(" ");
			    txt2a.addElement(txt2a_array);
			  }
			br_txt2a.close();
		}
	    catch (Exception e){
	    	System.out.println(e.toString());
	    	System.out.println("Erreur txt2a");
	    }
	    numtxt2a = numtxt2a + 1;

		  //Extract information from query_2b.txt
	    InputStream fr_txt2b;
	    try{
			fr_txt2b = new FileInputStream("../../../Output/query_2b.txt");
			InputStreamReader lct_txt2b = new InputStreamReader(fr_txt2b);
			BufferedReader br_txt2b = new BufferedReader(lct_txt2b);
			String line_txt2b;
			String[] txt2b_array;

			while ((line_txt2b = br_txt2b.readLine()) != null ) {
			    txt2b_array = line_txt2b.split(" ");
			    txt2b.addElement(txt2b_array);
			  }
			br_txt2b.close();
		}
	    catch (Exception e){
	    	System.out.println(e.toString());
	    	System.out.println("Erreur txt2b");
	    }

		  //Extract information from query_2c.txt
	    InputStream fr_txt2c;
	    try{
			fr_txt2c = new FileInputStream("../../../Output/query_2c.txt");
			InputStreamReader lct_txt2c = new InputStreamReader(fr_txt2c);
			BufferedReader br_txt2c = new BufferedReader(lct_txt2c);
			String line_txt2c;
			String[] txt2c_array;

			while ((line_txt2c = br_txt2c.readLine()) != null ) {
			    txt2c_array = line_txt2c.split(" ");
			    txt2c.addElement(txt2c_array);
			    numtxt2c = numtxt2c + 1;
			  }
			br_txt2c.close();
		}
	    catch (Exception e){
	    	System.out.println(e.toString());
	    	System.out.println("Erreur txt2c");
	    }


	    //Init of variables which contain number of column in query.
	    //It is not possible to change position of table
		   selectLeft1a = Integer.parseInt(((String[])txt1a.elementAt(0))[0].substring(2, 3));
		   selectRight1a = Integer.parseInt(((String[])txt1a.elementAt(0))[1].substring(2, 3));
		   conditionLeft1a = Integer.parseInt(((String[])txt1a.elementAt(2))[0].substring(2, 3));
		   conditionRight1a = Integer.parseInt(((String[])txt1a.elementAt(2))[2].substring(2, 3));
		   op1a = Integer.parseInt(((String[])txt1a.elementAt(2))[1]);


		   selectLeft1b = Integer.parseInt(((String[])txt1b.elementAt(0))[0].substring(2, 3));
		   selectRight1b = Integer.parseInt(((String[])txt1b.elementAt(0))[1].substring(2, 3));
		   condition1Left1b = Integer.parseInt(((String[])txt1b.elementAt(2))[0].substring(2, 3));
		   op11b = Integer.parseInt(((String[])txt1b.elementAt(2))[1]);
		   condition1Right1b = Integer.parseInt(((String[])txt1b.elementAt(2))[2].substring(2, 3));
		   condition2Left1b = Integer.parseInt(((String[])txt1b.elementAt(4))[0].substring(2, 3));
		   op21b = Integer.parseInt(((String[])txt1b.elementAt(4))[1]);
		   condition2Right1b = Integer.parseInt(((String[])txt1b.elementAt(4))[2].substring(2, 3));
		   rac1b = ((String[])txt1b.elementAt(3))[0];


		   selectLeft2a = Integer.parseInt(((String[])txt2a.elementAt(0))[0].substring(2, 3));
		   selectRight2a = Integer.parseInt(((String[])txt2a.elementAt(0))[1].substring(2, 3));
		   conditionLeft2a = Integer.parseInt(((String[])txt2a.elementAt(2))[0].substring(2, 3));
		   conditionRight2a = Integer.parseInt(((String[])txt2a.elementAt(2))[2].substring(2, 3));
		   op2a = Integer.parseInt(((String[])txt2a.elementAt(2))[1]);


		   selectLeft2b = Integer.parseInt(((String[])txt2b.elementAt(0))[0].substring(2, 3));
		   selectRight2b = Integer.parseInt(((String[])txt2b.elementAt(0))[1].substring(2, 3));
		   condition1Left2b = Integer.parseInt(((String[])txt2b.elementAt(2))[0].substring(2, 3));
		   op12b = Integer.parseInt(((String[])txt2b.elementAt(2))[1]);
		   condition1Right2b = Integer.parseInt(((String[])txt2b.elementAt(2))[2].substring(2, 3));
		   condition2Left2b = Integer.parseInt(((String[])txt2b.elementAt(4))[0].substring(2, 3));
		   op22b = Integer.parseInt(((String[])txt2b.elementAt(4))[1]);
		   condition2Right2b = Integer.parseInt(((String[])txt2b.elementAt(4))[2].substring(2, 3));
		   rac2b = ((String[])txt2b.elementAt(3))[0];

		   selectLeft2c = Integer.parseInt(((String[])txt2c.elementAt(0))[0].substring(2, 3));
		   selectRight2c = Integer.parseInt(((String[])txt2c.elementAt(0))[1].substring(2, 3));
		   condition1Left2c = Integer.parseInt(((String[])txt2c.elementAt(2))[0].substring(2, 3));
		   op12c = Integer.parseInt(((String[])txt2c.elementAt(2))[1]);
		   condition1Right2c = Integer.parseInt(((String[])txt2c.elementAt(2))[2].substring(2, 3));
		   condition2Left2c = Integer.parseInt(((String[])txt2c.elementAt(4))[0].substring(2, 3));
		   op22c = Integer.parseInt(((String[])txt2c.elementAt(4))[1]);
		   condition2Right2c = Integer.parseInt(((String[])txt2c.elementAt(4))[2].substring(2, 3));
		   rac2c = ((String[])txt2c.elementAt(3))[0];
		   nameBdd1 = ((String[])txt2c.elementAt(0))[0].substring(0, 1).toLowerCase();
		   nameBdd2 = ((String[])txt2c.elementAt(0))[1].substring(0, 1).toLowerCase();




	    InputStream fr_R;
	    try{
			fr_R = new FileInputStream("../../../Output/R.txt");
			InputStreamReader lct_R = new InputStreamReader(fr_R);
			BufferedReader br_R = new BufferedReader(lct_R);
			String line_R;
			String[] R_array;
			line_R = br_R.readLine();
			while ((line_R = br_R.readLine()) != null ) {
			    R_array = line_R.split(",");
			    r.addElement( new R(Integer.valueOf(R_array[0]), Integer.valueOf(R_array[1]), Integer.valueOf(R_array[2]), Integer.valueOf(R_array[3]) ));
			  }
			br_R.close();
		}
	    catch (Exception e){
	    	System.out.println(e.toString());
	    	System.out.println("Erreur R");
	    }
	    numR = r.size();

	    InputStream fr_S;
		try {
			fr_S = new FileInputStream("../../../Output/S.txt");
			InputStreamReader lct_S = new InputStreamReader(fr_S);
		    BufferedReader br_S = new BufferedReader(lct_S);
		    String line_S;
		    String[] S_array;
		    line_S = br_S.readLine();
		    while ((line_S = br_S.readLine()) != null ) {
		        S_array = line_S.split(",");
		        s.addElement( new S(Integer.valueOf(S_array[0]), Integer.valueOf(S_array[1]), Integer.valueOf(S_array[2]), Integer.valueOf(S_array[3])) );

		    }
		    br_S.close();
		}
		catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
			System.out.println("Erreur S");
		}
	    numS = s.size();


	    InputStream fr_Q;
		try {
			fr_Q = new FileInputStream("../../../Output/q.txt");
			InputStreamReader lct_Q = new InputStreamReader(fr_Q);
		    BufferedReader br_Q = new BufferedReader(lct_Q);
		    String line_Q;
		    String[] Q_array;
		    line_Q = br_Q.readLine();
		    while ((line_Q = br_Q.readLine()) != null && numq < 100) {
		    	numq = numq+1;
		        Q_array = line_Q.split(",");
		        q.addElement( new Q(Integer.valueOf(Q_array[0]), Integer.valueOf(Q_array[1]), Integer.valueOf(Q_array[2]), Integer.valueOf(Q_array[3])) );

		    }
		    br_Q.close();
		}
		catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
			System.out.println("Erreur Q");
		}
		numq = q.size();

	    boolean status = OK;

	    int numS_attrs = 4;
	    int numR_attrs = 4;
	    int numq_attrs = 4;


	    String dbpath = "/tmp/"+System.getProperty("user.name")+".minibase.jointestdb";
	    String logpath = "/tmp/"+System.getProperty("user.name")+".joinlog";

	    String remove_cmd = "/bin/rm -rf ";
	    String remove_logcmd = remove_cmd + logpath;
	    String remove_dbcmd = remove_cmd + dbpath;
	    String remove_joincmd = remove_cmd + dbpath;

	    try {
	      Runtime.getRuntime().exec(remove_logcmd);
	      Runtime.getRuntime().exec(remove_dbcmd);
	      Runtime.getRuntime().exec(remove_joincmd);
	    }
	    catch (IOException e) {
	      System.err.println (""+e);
	    }


	    SystemDefs sysdef = new SystemDefs( dbpath, 1000, NUMBUF, "Clock");

	    // creating the S relation
	    AttrType [] Stypes = {
	    	new AttrType (AttrType.attrInteger),
	    	new AttrType (AttrType.attrInteger),
	    	new AttrType (AttrType.attrInteger),
	    	new AttrType (AttrType.attrInteger)
	    };


	    short [] Ssizes = new short [1];
	    Ssizes[0] = 30;

	    Tuple t = new Tuple();
	    try {
	      t.setHdr((short) 4,Stypes, Ssizes);
	    }
	    catch (Exception e) {
	      System.err.println("*** error in Tuple.setHdr() ***");
	      status = FAIL;
	      e.printStackTrace();
	    }

	    int size = t.size();

	    RID             rid;
	    Heapfile        f = null;
	    try {
	      f = new Heapfile("s.in");
	    }
	    catch (Exception e) {
	      System.err.println("*** error in Heapfile constructor ***");
	      status = FAIL;
	      e.printStackTrace();
	    }

	    t = new Tuple(size);
	    try {
	      t.setHdr((short) 4, Stypes, Ssizes);
	    }
	    catch (Exception e) {
	      System.err.println("*** error in Tuple.setHdr() ***");
	      status = FAIL;
	      e.printStackTrace();
	    }

	    for (int i=0; i<numS; i++) {
	      try {
		t.setIntFld(1, ((S)s.elementAt(i)).col1);
		t.setIntFld(2, ((S)s.elementAt(i)).col2);
		t.setIntFld(3, ((S)s.elementAt(i)).col3);
		t.setIntFld(4, ((S)s.elementAt(i)).col4);
	      }
	      catch (Exception e) {
		System.err.println("*** Heapfile error in Tuple.setStrFld() ***");
		status = FAIL;
		e.printStackTrace();
	      }

	      try {
		rid = f.insertRecord(t.returnTupleByteArray());
	      }
	      catch (Exception e) {
		System.err.println("*** error in Heapfile.insertRecord() ***");
		status = FAIL;
		e.printStackTrace();
	      }
	    }
	    if (status != OK) {
	      //bail out
	      System.err.println ("*** Error creating relation for S");
	      Runtime.getRuntime().exit(1);
	    }

	    //creating the R relation
	    AttrType [] Rtypes = new AttrType[4];
	    Rtypes[0] =  new AttrType(AttrType.attrInteger);
	    Rtypes[1] =  new AttrType(AttrType.attrInteger);
	    Rtypes[2] =  new AttrType(AttrType.attrInteger);
	    Rtypes[3] =  new AttrType(AttrType.attrInteger);


	    short  []  Rsizes = new short[1];
	    Rsizes[0] = 0;
	    t = new Tuple();
	    try {
	      t.setHdr((short) 4,Rtypes, Rsizes);
	    }
	    catch (Exception e) {
	      System.err.println("*** error in Tuple.setHdr() ***");
	      status = FAIL;
	      e.printStackTrace();
	    }

	    size = t.size();

	    // inserting the tuple into file r
	    //RID             rid;
	    f = null;
	    try {
	      f = new Heapfile("r.in");
	    }
	    catch (Exception e) {
	      System.err.println("*** error in Heapfile constructor ***");
	      status = FAIL;
	      e.printStackTrace();
	    }

	    t = new Tuple(size);
	    try {
	      t.setHdr((short) 4, Rtypes, Rsizes);
	    }
	    catch (Exception e) {
	      System.err.println("*** error in Tuple.setHdr() ***");
	      status = FAIL;
	      e.printStackTrace();
	    }

	    for (int i=0; i<numR; i++) {
	      try {
		t.setIntFld(1, ((R)r.elementAt(i)).col1);
		t.setIntFld(2, ((R)r.elementAt(i)).col2);
		t.setIntFld(3, ((R)r.elementAt(i)).col3);
		t.setIntFld(4, ((R)r.elementAt(i)).col4);
	      }
	      catch (Exception e) {
		System.err.println("*** error in Tuple.setStrFld() ***");
		status = FAIL;
		e.printStackTrace();
	      }

	      try {
		rid = f.insertRecord(t.returnTupleByteArray());
	      }
	      catch (Exception e) {
		System.err.println("*** error in Heapfile.insertRecord() ***");
		status = FAIL;
		e.printStackTrace();
	      }
	    }
	    if (status != OK) {
	      //bail out
	      System.err.println ("*** Error creating relation for R");
	      Runtime.getRuntime().exit(1);
	    }


	    //creating Q relation
	    AttrType [] Qtypes = new AttrType[4];
	    Qtypes[0] = new AttrType (AttrType.attrInteger);
	    Qtypes[1] = new AttrType (AttrType.attrInteger);
	    Qtypes[2] = new AttrType (AttrType.attrInteger);
	    Qtypes[3] = new AttrType (AttrType.attrInteger);

	    short [] Qsizes = new short [1];
	    Qsizes[0] = 0;
	    t = new Tuple();
	    try {
	      t.setHdr((short) 4,Qtypes, Qsizes);
	    }
	    catch (Exception e) {
	      System.err.println("*** error in Tuple.setHdr() ***");
	      status = FAIL;
	      e.printStackTrace();
	    }

	    size = t.size();

	    // inserting the tuple into file q
	    //RID             rid;
	    f = null;
	    try {
	      f = new Heapfile("q.in");
	    }
	    catch (Exception e) {
	      System.err.println("*** error in Heapfile constructor ***");
	      status = FAIL;
	      e.printStackTrace();
	    }

	    t = new Tuple(size);
	    try {
	      t.setHdr((short) 4, Qtypes, Qsizes);
	    }
	    catch (Exception e) {
	      System.err.println("*** error in Tuple.setHdr() ***");
	      status = FAIL;
	      e.printStackTrace();
	    }

	    for (int i=0; i<numq; i++) {
	      try {
		t.setIntFld(1, ((Q)q.elementAt(i)).col1);
		t.setIntFld(2, ((Q)q.elementAt(i)).col2);
		t.setIntFld(3, ((Q)q.elementAt(i)).col3);
		t.setIntFld(4, ((Q)q.elementAt(i)).col4);

	      }
	      catch (Exception e) {
		System.err.println("*** error in Tuple.setStrFld() ***");
		status = FAIL;
		e.printStackTrace();
	      }

	      try {
		rid = f.insertRecord(t.returnTupleByteArray());
	      }
	      catch (Exception e) {
		System.err.println("*** error in Heapfile.insertRecord() ***");
		status = FAIL;
		e.printStackTrace();
	      }
	    }
	    if (status != OK) {
	      //bail out
	      System.err.println ("*** Error creating relation for Q");
	      Runtime.getRuntime().exit(1);

	    }

	  }



  public boolean runTests() {
	  Disclaimer();

	  //Here you can only one query by one, not simultaneously

		  			//Query1a();

		  		    //Query1b();

		  		    //Query2a();

		  		    Query2b();

			  	    //Query2c();






		System.out.print ("Finished joins testing"+"\n");


		    return true;
			}


  private void Query1a_CondExpr(CondExpr[] expr) {

	expr[0].next  = null;
    expr[0].op    = new AttrOperator(op1a);
    expr[0].type1 = new AttrType(AttrType.attrSymbol);
    expr[0].type2 = new AttrType(AttrType.attrSymbol);
    expr[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),conditionLeft1a);
    expr[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),conditionRight1a);


    expr[1] = null;
  }


  private void Query1b_CondExpr(CondExpr[] expr) {

	if (rac1b.compareTo("AND")==0) {
	    expr[0].next  = null;
	    expr[0].op    = new AttrOperator(op11b);
	    expr[0].type1 = new AttrType(AttrType.attrSymbol);
	    expr[0].type2 = new AttrType(AttrType.attrSymbol);
	    expr[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),condition1Left1b);
	    expr[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),condition1Right1b);

	    expr[1].op    = new AttrOperator(op21b);
	    expr[1].next  = null;
	    expr[1].type1 = new AttrType(AttrType.attrSymbol);
	    expr[1].type2 = new AttrType(AttrType.attrSymbol);
	    expr[1].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer), condition2Left1b);
	    expr[1].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),condition2Right1b);

	    expr[2] = null;
	}
	else if (rac1b.compareTo("OR")==0) {
	    expr[0].next = null;
	    expr[0].op   = new AttrOperator(op11b);
	    expr[0].type1 = new AttrType(AttrType.attrSymbol);
	    expr[0].type2 = new AttrType(AttrType.attrSymbol);
	    expr[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),condition1Left1b);
	    expr[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),condition1Right1b);

	    expr[0].next = new CondExpr();
	    expr[0].next.op   = new AttrOperator(op21b);
	    expr[0].next.type1 = new AttrType(AttrType.attrSymbol);
	    expr[0].next.type2 = new AttrType(AttrType.attrSymbol);
	    expr[0].next.operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),condition2Left1b);
	    expr[0].next.operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),condition2Right1b);
	    expr[0].next.next = null;

	    expr[1] = null;
	}
	else {
		System.out.println("At the line 4, it can only be OR or AND");
	}
  }

  private void Query2a_CondExpr(CondExpr[] expr) {

	    expr[0].next  = null;
	    expr[0].op    = new AttrOperator(op2a);
	    expr[0].type1 = new AttrType(AttrType.attrSymbol);
	    expr[0].type2 = new AttrType(AttrType.attrSymbol);
	    expr[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),conditionLeft2a);
	    expr[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),conditionRight2a);

	    expr[1] = null;
	  }

  private void Query2b_CondExpr(CondExpr[] expr) {

	if (rac2b.compareTo("AND")==0) {
	    expr[0].next  = null;
	    expr[0].op    = new AttrOperator(op12b);
	    expr[0].type1 = new AttrType(AttrType.attrSymbol);
	    expr[0].type2 = new AttrType(AttrType.attrSymbol);
	    expr[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),condition1Left2b);
	    expr[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),condition1Right2b);

	    expr[1].op    = new AttrOperator(op22b);
	    expr[1].next  = null;
	    expr[1].type1 = new AttrType(AttrType.attrSymbol);
	    expr[1].type2 = new AttrType(AttrType.attrSymbol);
	    expr[1].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer), condition2Left2b);
	    expr[1].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),condition2Right2b);

	    expr[2] = null;
	}
	else if (rac2b.compareTo("OR")==0) {
		System.out.println("OR not yet implemented, please keep AND");
	    expr[0].next = null;
	    expr[0].op   = new AttrOperator(op12b);
	    expr[0].type1 = new AttrType(AttrType.attrSymbol);
	    expr[0].type2 = new AttrType(AttrType.attrSymbol);
	    expr[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),condition1Left2b);
	    expr[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),condition1Right2b);

	    expr[0].next = new CondExpr();
	    expr[0].next.op   = new AttrOperator(op22b);
	    expr[0].next.type1 = new AttrType(AttrType.attrSymbol);
	    expr[0].next.type2 = new AttrType(AttrType.attrSymbol);
	    expr[0].next.operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),condition2Left2b);
	    expr[0].next.operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),condition2Right2b);
	    expr[0].next.next = null;

	    expr[1] = null;
	}
	else {
		System.out.println("At the line 4, it can only be OR or AND");
	}
  }

  private void Query2c_CondExpr(CondExpr[] expr) {

		if (rac2c.compareTo("AND")==0) {
		    expr[0].next  = null;
		    expr[0].op    = new AttrOperator(op12c);
		    expr[0].type1 = new AttrType(AttrType.attrSymbol);
		    expr[0].type2 = new AttrType(AttrType.attrSymbol);
		    expr[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),condition1Left2c);
		    expr[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),condition1Right2c);

		    expr[1].op    = new AttrOperator(op22c);
		    expr[1].next  = null;
		    expr[1].type1 = new AttrType(AttrType.attrSymbol);
		    expr[1].type2 = new AttrType(AttrType.attrSymbol);
		    expr[1].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer), condition2Left2c);
		    expr[1].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),condition2Right2c);

		    expr[2] = null;
		}
		else if (rac2c.compareTo("OR")==0) {
			System.out.println("OR not yet implemented, please keep AND");
		    expr[0].next = null;
		    expr[0].op   = new AttrOperator(op12c);
		    expr[0].type1 = new AttrType(AttrType.attrSymbol);
		    expr[0].type2 = new AttrType(AttrType.attrSymbol);
		    expr[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),condition1Left2c);
		    expr[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),condition1Right2c);

		    expr[0].next = new CondExpr();
		    expr[0].next.op   = new AttrOperator(op22c);
		    expr[0].next.type1 = new AttrType(AttrType.attrSymbol);
		    expr[0].next.type2 = new AttrType(AttrType.attrSymbol);
		    expr[0].next.operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),condition2Left2c);
		    expr[0].next.operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),condition2Right2c);
		    expr[0].next.next = null;

		    expr[1] = null;
		}
		else {
			System.out.println("At the line 4, it can only be OR or AND");
		}
	  }


  public void Query1a() {

	  AttrOperator symbol1a = new AttrOperator(op1a);


	  System.out.print("**********************Query1a strating *********************\n");
	  System.out.print("  SELECT   R.col_" + selectLeft1a + ", S.col_" + selectRight1a + "\n"
	        + "  FROM     R, S\n"
	        + "  WHERE    R.col_" + conditionLeft1a + " " + symbol1a.toSymbol() + " " + " S.col" + conditionRight1a + "\n\n");

    boolean status = OK;

    // Build Index first
    IndexType b_index1a = new IndexType (IndexType.B_Index);


    //ExtendedSystemDefs.MINIBASE_CATALOGPTR.addIndex("sailors.in", "sid", b_index, 1);
    // }
    //catch (Exception e) {
    // e.printStackTrace();
    // System.err.print ("Failure to add index.\n");
      //  Runtime.getRuntime().exit(1);
    // }

    CondExpr [] outFilter  = new CondExpr[2];
    outFilter[0] = new CondExpr();
    outFilter[1] = new CondExpr();

    Query1a_CondExpr(outFilter);
    Tuple t = new Tuple();
    t = null;

    // R table
    AttrType [] Rtypes = {
      new AttrType(AttrType.attrInteger),
      new AttrType(AttrType.attrInteger),
      new AttrType(AttrType.attrInteger),
      new AttrType(AttrType.attrInteger),
    };

    short  []  Rsizes = new short[1] ;
    Rsizes[0] = 30;


    // S table
    AttrType [] Stypes = {
      new AttrType(AttrType.attrInteger),
      new AttrType(AttrType.attrInteger),
      new AttrType(AttrType.attrInteger),
      new AttrType(AttrType.attrInteger)
    };

    short []   Ssizes = new short[1];
    Ssizes[0] = 30;


    // Final table
    AttrType [] Jtypes1a = {
      new AttrType(AttrType.attrInteger),
      new AttrType(AttrType.attrInteger),
    };

    short []   Jsizes1a = new short[1];
    Jsizes1a[0] = 30;


    FldSpec [] Rprojection = {
    	       new FldSpec(new RelSpec(RelSpec.outer), 1),
    	       new FldSpec(new RelSpec(RelSpec.outer), 2),
    	       new FldSpec(new RelSpec(RelSpec.outer), 3),
    	       new FldSpec(new RelSpec(RelSpec.outer), 4),
    	    };


    FldSpec [] proj11a = {
 	       new FldSpec(new RelSpec(RelSpec.outer), selectLeft1a),
 	       new FldSpec(new RelSpec(RelSpec.innerRel), selectRight1a),
 	    };

    CondExpr [] selects = new CondExpr[1];
    selects[0] = null;


    //IndexType b_index = new IndexType(IndexType.B_Index);
    iterator.Iterator am1a = null;


    //_______________________________________________________________
    //*******************create an scan on the heapfile**************
    //+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    // create a tuple of appropriate size
        Tuple tt = new Tuple();
    try {
      tt.setHdr((short) 4, Rtypes, Rsizes);
    }
    catch (Exception e) {
      status = FAIL;
      e.printStackTrace();
    }

    int sizett = tt.size();
    tt = new Tuple(sizett);
    try {
      tt.setHdr((short) 4, Rtypes, Rsizes);
    }
    catch (Exception e) {
      status = FAIL;
      e.printStackTrace();
    }
    Heapfile        f = null;
    try {
      f = new Heapfile("r.in");
    }
    catch (Exception e) {
      status = FAIL;
      e.printStackTrace();
    }

    Scan scan = null;

    try {
      scan = new Scan(f);
    }
    catch (Exception e) {
      status = FAIL;
      e.printStackTrace();
      Runtime.getRuntime().exit(1);
    }

    // create the index file
    BTreeFile btf = null;
    try {
      btf = new BTreeFile("BTreeIndex", AttrType.attrInteger, 4, 1);
    }
    catch (Exception e) {
      status = FAIL;
      e.printStackTrace();
      Runtime.getRuntime().exit(1);
    }

    RID rid = new RID();
    int key =0;
    Tuple temp = null;

    try {
      temp = scan.getNext(rid);
    }
    catch (Exception e) {
      status = FAIL;
      e.printStackTrace();
    }
    while ( temp != null) {
      tt.tupleCopy(temp);

      try {
	key = tt.getIntFld(1);
      }
      catch (Exception e) {
	status = FAIL;
	e.printStackTrace();
      }

      try {
	btf.insert(new IntegerKey(key), rid);
      }
      catch (Exception e) {
	status = FAIL;
	e.printStackTrace();
      }

      try {
	temp = scan.getNext(rid);
      }
      catch (Exception e) {
	status = FAIL;
	e.printStackTrace();
      }
    }

    // close the file scan
    scan.closescan();


    //_______________________________________________________________
    //*******************close an scan on the heapfile**************
    //++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

    System.out.print ("After Building btree index on R.3.\n\n");
    try {
      am1a = new IndexScan ( b_index1a, "r.in",
			   "BTreeIndex", Rtypes, Rsizes, 4, 4,
			   Rprojection, null, 4, false);
    }

    catch (Exception e) {
      System.err.println ("*** Error creating scan for Index scan");
      System.err.println (""+e);
      Runtime.getRuntime().exit(1);
    }


    NestedLoopsJoins nlj1a = null;
    long duration = 0;
    try {

    	long startTime = System.nanoTime();


      nlj1a = new NestedLoopsJoins (Rtypes, 4, Rsizes,
				  Stypes, 4, Ssizes,
				  10,
				  am1a, "s.in",
				  outFilter, null, proj11a, 2);
      long endTime = System.nanoTime();

      duration = (endTime - startTime);

      System.out.println("The NestedLoopJoin algorithm for query1a takes "+ duration +" nanoseconds. \n");

    }
    catch (Exception e) {
      System.err.println ("*** Error preparing for nested_loop_join");
      System.err.println (""+e);
      e.printStackTrace();
      Runtime.getRuntime().exit(1);
    }

    try {
        PrintWriter writer = new PrintWriter("../../../Output/output_tuples/output_query1a.txt", "UTF-8");
        writer.println("**********************Query1a strating *********************\n");
        writer.println ("  SELECT   R.col_" + selectLeft1a + ", S.col_" + selectRight1a + "\n"
  	        + "  FROM     R, S\n"
  	        + "  WHERE    R.col_" + conditionLeft1a + " " + symbol1a.toSymbol() + " " + " S.col" + conditionRight1a + "\n\n");
        writer.println("The NestedLoopJoin algorithm for query1a takes "+ duration +" nanoseconds.\n\n");
        writer.println("Number of inputs : R=" + numR +" et S=" + numS + "\n\n");
        writer.println("Here are the output tuples for query 1a : \n\n");

        t = null;
        int nbOut = 0;
        try {
          while ((t = nlj1a.get_next()) != null) {
            t.print(Jtypes1a);
            writer.println(t.getIntFld(1)+" "+t.getIntFld(2)+"\n");
            nbOut = nbOut + 1;
          }
        }
        catch (Exception e) {
          System.err.println (""+e);
          e.printStackTrace();
          Runtime.getRuntime().exit(1);
        }
        writer.println("Number of outputs : " + nbOut + " tuples");
        writer.close();
    }
    catch (Exception e)
    {
        System.out.println("Error when creating the file of output tuples");
    }




    System.out.println ("\n");
    try {
    	nlj1a.close();
    }
    catch (Exception e) {
      status = FAIL;
      e.printStackTrace();
    }

    if (status != OK) {
      //bail out

      Runtime.getRuntime().exit(1);}
      }



    public void Query1b() {

    	AttrOperator symbol11b = new AttrOperator(op11b);
		AttrOperator symbol21b = new AttrOperator(op21b);

    	System.out.print("**********************Query1b strating *********************\n");
    	System.out.print("  SELECT   R.col_" + selectLeft1b + ", S.col_" + selectRight1b + "\n"
  	        + "  FROM     R, S\n"
  	        + "  WHERE   R.col_" + condition1Left1b + " " + symbol11b.toSymbol() + " S.col_" + condition1Right1b
  	        + " " + rac1b + " R.col_" + condition2Left1b + " " + symbol21b.toSymbol() + " S.col_" + condition2Right1b );

        boolean status = OK;

        // S, R, Q Queries.
        /*
        System.out.print ("Query: \n"
    		      + "  SELECT R.col1, S.col1\n"
    		      + "  FROM   S, R\n"
    		      + "  WHERE  R.col3 < S.col3"
    		      + "         AND "
    		      + "         R.col4 <= S.col4\n\n");
        */


        // Build Index first
        IndexType b_index1b = new IndexType (IndexType.B_Index);

    	String and = new String("AND");
    	String or = new String("OR");
        CondExpr [] outFilter_and  = new CondExpr[3];
        CondExpr [] outFilter_or  = new CondExpr[2];


    	if (rac1b.compareTo("AND")==0) {
            outFilter_and[0] = new CondExpr();
            outFilter_and[1] = new CondExpr();
            outFilter_and[2] = new CondExpr();
            Query1b_CondExpr(outFilter_and);
    	}
    	else if (rac1b.compareTo("OR")==0) {
            outFilter_or[0] = new CondExpr();
            outFilter_or[1] = new CondExpr();
            Query1b_CondExpr(outFilter_or);
    	}


        Tuple t = new Tuple();
        t = null;

        // R table
        AttrType [] Rtypesb = {
          new AttrType(AttrType.attrInteger),
          new AttrType(AttrType.attrInteger),
          new AttrType(AttrType.attrInteger),
          new AttrType(AttrType.attrInteger),
        };

        short  []  Rsizesb = new short[1] ;
        Rsizesb[0] = 30;


        // S table
        AttrType [] Stypesb = {
          new AttrType(AttrType.attrInteger),
          new AttrType(AttrType.attrInteger),
          new AttrType(AttrType.attrInteger),
          new AttrType(AttrType.attrInteger)
        };

        short []   Ssizesb = new short[1];
        Ssizesb[0] = 30;


        // Final table
        AttrType [] Jtypesb = {
          new AttrType(AttrType.attrInteger),
          new AttrType(AttrType.attrInteger),
        };

        short []   Jsizesb = new short[1];
        Jsizesb[0] = 30;

        FldSpec [] Rprojectionb = {
        	       new FldSpec(new RelSpec(RelSpec.outer), 1),
        	       new FldSpec(new RelSpec(RelSpec.outer), 2),
        	       new FldSpec(new RelSpec(RelSpec.outer), 3),
        	       new FldSpec(new RelSpec(RelSpec.outer), 4),
        	    };


        FldSpec [] projectionfin = {
     	       new FldSpec(new RelSpec(RelSpec.outer), selectLeft1b),
     	       new FldSpec(new RelSpec(RelSpec.innerRel), selectRight1b),
     	    };


        CondExpr [] selects = new CondExpr[1];
        selects[0] = null;


        //IndexType b_index = new IndexType(IndexType.B_Index);
        iterator.Iterator am1b = null;


        //_______________________________________________________________
        //*******************create an scan on the heapfile**************
        //+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
        // create a tuple of appropriate size
            Tuple ttb = new Tuple();
        try {
          ttb.setHdr((short) 4, Rtypesb, Rsizesb);
        }
        catch (Exception e) {
          status = FAIL;
          e.printStackTrace();
        }

        int sizett = ttb.size();
        ttb = new Tuple(sizett);
        try {
          ttb.setHdr((short) 4, Rtypesb, Rsizesb);
        }
        catch (Exception e) {
          status = FAIL;
          e.printStackTrace();
        }
        Heapfile        f = null;
        try {
          f = new Heapfile("r.in");
        }
        catch (Exception e) {
          status = FAIL;
          e.printStackTrace();
        }

        Scan scan = null;

        try {
          scan = new Scan(f);
        }
        catch (Exception e) {
          status = FAIL;
          e.printStackTrace();
          Runtime.getRuntime().exit(1);
        }

        // create the index file
        BTreeFile btf = null;
        try {
          btf = new BTreeFile("BTreeIndex", AttrType.attrInteger, 4, 1);
        }
        catch (Exception e) {
          status = FAIL;
          e.printStackTrace();
          Runtime.getRuntime().exit(1);
        }

        RID rid = new RID();
        int key =0;
        Tuple temp = null;

        try {
          temp = scan.getNext(rid);
        }
        catch (Exception e) {
          status = FAIL;
          e.printStackTrace();
        }
        while ( temp != null) {
          ttb.tupleCopy(temp);

          try {
    	key = ttb.getIntFld(1);
          }
          catch (Exception e) {
    	status = FAIL;
    	e.printStackTrace();
          }

          try {
    	btf.insert(new IntegerKey(key), rid);
          }
          catch (Exception e) {
    	status = FAIL;
    	e.printStackTrace();
          }

          try {
    	temp = scan.getNext(rid);
          }
          catch (Exception e) {
    	status = FAIL;
    	e.printStackTrace();
          }
        }

        // close the file scan
        scan.closescan();


        //_______________________________________________________________
        //*******************close an scan on the heapfile**************
        //++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

        System.out.print ("After Building btree index on R.3.\n\n");
        try {
          am1b = new IndexScan ( b_index1b, "r.in",
    			   "BTreeIndex", Rtypesb, Rsizesb, 4, 4,
    			   Rprojectionb, null, 1, false);
        }

        catch (Exception e) {
          System.err.println ("*** Error creating scan for Index scan");
          System.err.println (""+e);
          Runtime.getRuntime().exit(1);
        }


        NestedLoopsJoins nlj1b = null;
        long duration = 0;
		try {

        		long startTime = System.nanoTime();

                nlj1b = new NestedLoopsJoins (Rtypesb, 4, Rsizesb,
      				  Stypesb, 4, Ssizesb,
      				  10,
      				  am1b, "s.in",
      				  outFilter_and, null, projectionfin, 2);

                long endTime = System.nanoTime();

                duration  = (endTime - startTime);

                System.out.println("The NestedLoopJoin algorithm for query1a takes "+ duration +" nanoseconds. \n");
        }
        catch (Exception e) {
          System.err.println ("*** Error preparing for nested_loop_join");
          System.err.println (""+e);
          e.printStackTrace();
          Runtime.getRuntime().exit(1);
        }


        try {
            PrintWriter writer = new PrintWriter("../../../Output/output_tuples/output_query1b.txt", "UTF-8");
            writer.println("**********************Query1b strating *********************\n");
            writer.println ("  SELECT   R.col_" + selectLeft1b + ", S.col_" + selectRight1b + "\n"
      	        + "  FROM     R, S\n"
      	        + "  WHERE   R.col_" + condition1Left1b + " " + symbol11b.toSymbol() + " S.col_" + condition1Right1b
      	        + " " + rac1b + " R.col_" + condition2Left1b + " " + symbol21b.toSymbol() + " S.col_" + condition2Right1b );
            writer.println("The NestedLoopJoin algorithm for query1b takes "+ duration +" nanoseconds.\n\n");
            writer.println("Number of inputs : R=" + numR +" et S=" + numS + "\n\n");
            writer.println("Here are the output tuples for query 1b : \n\n");

            t = null;
            int nbOut = 0;
            try {
              while ((t = nlj1b.get_next()) != null) {
                t.print(Jtypesb);
                writer.println(t.getIntFld(1)+" "+t.getIntFld(2)+"\n");
                nbOut = nbOut + 1;
              }
            }
            catch (Exception e) {
              System.err.println (""+e);
              e.printStackTrace();
              Runtime.getRuntime().exit(1);
            }

            writer.println("Number of outputs : " + nbOut + " tuples");
            writer.close();
        }
        catch (Exception e)
        {
            System.out.println("Error when creating the file of output tuples");
        }

          System.out.println ("\n");
          try {
          	nlj1b.close();
          }
          catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
          }


          if (status != OK) {
            //bail out

            Runtime.getRuntime().exit(1);}
    }



    public void Query2a() {

    	AttrOperator symbol2a = new AttrOperator(op2a);

    	System.out.print("**********************Query2a strating *********************\n");
		    System.out.print ("  SELECT   Q.col_" + selectLeft2a + ", Q.col_" + selectRight2a + "\n"
  	        + "  FROM     Q\n"
  	        + "  WHERE    Q.col_" + conditionLeft2a + " " + symbol2a.toSymbol() + " " + " Q.col" + conditionRight2a + "\n\n");

        boolean status = OK;

        // Build Index first
        IndexType b_index = new IndexType (IndexType.B_Index);


        //ExtendedSystemDefs.MINIBASE_CATALOGPTR.addIndex("sailors.in", "sid", b_index, 1);
        // }
        //catch (Exception e) {
        // e.printStackTrace();
        // System.err.print ("Failure to add index.\n");
          //  Runtime.getRuntime().exit(1);
        // }

        CondExpr [] outFilter  = new CondExpr[2];
        outFilter[0] = new CondExpr();
        outFilter[1] = new CondExpr();

        Query2a_CondExpr(outFilter);
        Tuple t = new Tuple();
        t = null;


        // Q table
        AttrType [] qtypes = {
          new AttrType(AttrType.attrInteger),
          new AttrType(AttrType.attrInteger),
          new AttrType(AttrType.attrInteger),
          new AttrType(AttrType.attrInteger),
        };

        short  []  qsizes = new short[1] ;
        qsizes[0] = 30;

     // Final table
        AttrType [] Jtypes = {
          new AttrType(AttrType.attrInteger),
          new AttrType(AttrType.attrInteger),
        };

        short []   Jsizes = new short[1];
        Jsizes[0] = 30;

        FldSpec [] qprojection = {
        	       new FldSpec(new RelSpec(RelSpec.outer), 1),
        	       new FldSpec(new RelSpec(RelSpec.outer), 2),
        	       new FldSpec(new RelSpec(RelSpec.outer), 3),
        	       new FldSpec(new RelSpec(RelSpec.outer), 4),
        	    };


        FldSpec [] proj1 = {
     	       new FldSpec(new RelSpec(RelSpec.outer), selectLeft2a),
     	       new FldSpec(new RelSpec(RelSpec.innerRel), selectRight2a),
     	    };

        CondExpr [] selects = new CondExpr[1];
        selects[0] = null;


        //IndexType b_index = new IndexType(IndexType.B_Index);
        iterator.Iterator am = null;


        //_______________________________________________________________
        //*******************create an scan on the heapfile**************
        //+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
        // create a tuple of appropriate size
            Tuple tt = new Tuple();
        try {
          tt.setHdr((short) 4, qtypes, qsizes);
        }
        catch (Exception e) {
          status = FAIL;
          e.printStackTrace();
        }

        int sizett = tt.size();
        tt = new Tuple(sizett);
        try {
          tt.setHdr((short) 4, qtypes, qsizes);
        }
        catch (Exception e) {
          status = FAIL;
          e.printStackTrace();
        }
        Heapfile        f = null;
        try {
          f = new Heapfile("q.in");
        }
        catch (Exception e) {
          status = FAIL;
          e.printStackTrace();
        }

        Scan scan = null;

        try {
          scan = new Scan(f);
        }
        catch (Exception e) {
          status = FAIL;
          e.printStackTrace();
          Runtime.getRuntime().exit(1);
        }

        // create the index file
        BTreeFile btf = null;
        try {
          btf = new BTreeFile("BTreeIndex", AttrType.attrInteger, 4, 1);
        }
        catch (Exception e) {
          status = FAIL;
          e.printStackTrace();
          Runtime.getRuntime().exit(1);
        }

        RID rid = new RID();
        int key =0;
        Tuple temp = null;

        try {
          temp = scan.getNext(rid);
        }
        catch (Exception e) {
          status = FAIL;
          e.printStackTrace();
        }
        while ( temp != null) {
          tt.tupleCopy(temp);

          try {
    	key = tt.getIntFld(1);
          }
          catch (Exception e) {
    	status = FAIL;
    	e.printStackTrace();
          }

          try {
    	btf.insert(new IntegerKey(key), rid);
          }
          catch (Exception e) {
    	status = FAIL;
    	e.printStackTrace();
          }

          try {
    	temp = scan.getNext(rid);
          }
          catch (Exception e) {
    	status = FAIL;
    	e.printStackTrace();
          }
        }

        // close the file scan
        scan.closescan();


        //_______________________________________________________________
        //*******************close an scan on the heapfile**************
        //++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

        System.out.print ("After Building btree index on q.3.\n\n");
        try {
          am = new IndexScan ( b_index, "q.in",
    			   "BTreeIndex", qtypes, qsizes, 4, 4,
    			   qprojection, null, 4, false);
        }

        catch (Exception e) {
          System.err.println ("*** Error creating scan for Index scan");
          System.err.println (""+e);
          Runtime.getRuntime().exit(1);
        }


        IESelfJoin2a all_results2a = null;
        //NestedLoopsJoins nlj = null;
        long duration_nlj = 0;
        long duration_ieself = 0;


        try {

        	long startTime = System.nanoTime();
          all_results2a = new IESelfJoin2a (qtypes, 4, qsizes,
      				  10, am, "q.in", outFilter, proj1, 2);

          long ieselfendTime = System.nanoTime();
          /*
          nlj = new NestedLoopsJoins (qtypes, 4, qsizes,
				  qtypes, 4, qsizes,
				  10, am, "q.in", outFilter, null, proj1, 2);
          */
          long nljendTime = System.nanoTime();

          duration_nlj = nljendTime - ieselfendTime;

          duration_ieself = ieselfendTime - startTime;

          System.out.println("The IESelfjoin algorithm for query2a takes "+ duration_ieself +" nanoseconds. \n"
        		  + "The NestedLoopJoin algorithm for query2a takes "+ duration_nlj +" nanoseconds. \n");

        }
        catch (Exception e) {
          System.err.println ("*** Error preparing for IESelfJoin2a");
          System.err.println (""+e);
          e.printStackTrace();
          Runtime.getRuntime().exit(1);
        }



        try {
            PrintWriter writer = new PrintWriter("../../../Output/output_tuples/output_query2a.txt", "UTF-8");
            writer.println("**********************Query2a strating *********************\n");
            writer.println ("  SELECT   Q.col_" + selectLeft2a + ", Q.col_" + selectRight2a + "\n"
      	        + "  FROM     Q\n"
      	        + "  WHERE    Q.col_" + conditionLeft2a + " " + symbol2a.toSymbol() + " " + " Q.col" + conditionRight2a + "\n\n");
            writer.println("The IESelfjoin algorithm for query2a takes "+ duration_ieself +" nanoseconds. \n"
          		  + "The NestedLoopJoin algorithm for query2a takes "+ duration_nlj +" nanoseconds. \n");
            writer.println("Number of inputs : Q=" + numq + "\n\n");
            writer.println("Here are the output tuples for query 2a : \n\n");

            t = null;
            int nbOut = 0;

            try {
              while ((t = all_results2a.get_next()) != null) {
                t.print(Jtypes);
                writer.println(t.getIntFld(1)+" "+t.getIntFld(2)+"\n");
                nbOut = nbOut +1;
              }
            }
            catch (Exception e) {
              System.err.println (""+e);
              e.printStackTrace();
              Runtime.getRuntime().exit(1);
            }


            /*
            writer.println("NLJ : \n\n");
            try {
                while ((t = nlj.get_next()) != null) {
                  t.print(Jtypes);
                  writer.println(t.getIntFld(1)+" "+t.getIntFld(2)+"\n");
                }
              }
              catch (Exception e) {
                System.err.println (""+e);
              }
            */



            writer.println("Number of outputs : " + nbOut + " tuples");
            writer.close();
        }
        catch (Exception e)
        {
            System.out.println("Error when creating the file of output tuples");
        }

        t = null;

        System.out.println ("\n");
        try {
        	all_results2a.close();
        	//nlj.close();
        }
        catch (Exception e) {
          status = FAIL;
          e.printStackTrace();
        }

        if (status != OK) {
          //bail out

          Runtime.getRuntime().exit(1);}
          }


    public void Query2b() {

    	AttrOperator symbol12b = new AttrOperator(op12b);
		AttrOperator symbol22b = new AttrOperator(op22b);

    	System.out.print("**********************Query2b strating *********************\n");
    	System.out.print("  SELECT   R.col_" + selectLeft2b + ", S.col_" + selectRight2b + "\n"
  	        + "  FROM     R, S\n"
  	        + "  WHERE   R.col_" + condition1Left2b + " " + symbol12b.toSymbol() + " S.col_" + condition1Right2b
  	        + " " + rac2b + " R.col_" + condition2Left2b + " " + symbol22b.toSymbol() + " S.col_" + condition2Right2b );

        boolean status = OK;

        // Build Index first
        IndexType bb_index = new IndexType (IndexType.B_Index);
        IndexType bb_index2 = new IndexType (IndexType.B_Index);

        CondExpr [] outFilter = null;

    	if (rac2b.compareTo("AND")==0) {
    		outFilter = new CondExpr[3];
            outFilter[0] = new CondExpr();
            outFilter[1] = new CondExpr();
            outFilter[2] = new CondExpr();
            Query2b_CondExpr(outFilter);
    	}
    	else if (rac2b.compareTo("OR")==0) {
    		outFilter = new CondExpr[2];
            outFilter[0] = new CondExpr();
            outFilter[1] = new CondExpr();
            Query2b_CondExpr(outFilter);
    	}


        Tuple t = new Tuple();
        t = null;


        // Q table
        AttrType [] qtypes = {
          new AttrType(AttrType.attrInteger),
          new AttrType(AttrType.attrInteger),
          new AttrType(AttrType.attrInteger),
          new AttrType(AttrType.attrInteger),
        };

        short  []  qsizes = new short[1] ;
        qsizes[0] = 30;

     // Final table
        AttrType [] Jtypes = {
          new AttrType(AttrType.attrInteger),
          new AttrType(AttrType.attrInteger),
        };

        short []   Jsizes = new short[1];
        Jsizes[0] = 30;

        FldSpec [] qprojection = {
        	       new FldSpec(new RelSpec(RelSpec.outer), 1),
        	       new FldSpec(new RelSpec(RelSpec.outer), 2),
        	       new FldSpec(new RelSpec(RelSpec.outer), 3),
        	       new FldSpec(new RelSpec(RelSpec.outer), 4),
        	    };


        CondExpr [] selects = new CondExpr[1];
        selects[0] = null;



        FldSpec [] projectionfin = {
     	       new FldSpec(new RelSpec(RelSpec.outer), selectLeft2b),
     	       new FldSpec(new RelSpec(RelSpec.innerRel), selectRight2b),
     	    };



        //IndexType b_index = new IndexType(IndexType.B_Index);
        iterator.Iterator amb = null;
        iterator.Iterator amb2 = null;


        //_______________________________________________________________
        //*******************create an scan on the heapfile**************
        //+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
        // create a tuple of appropriate size
            Tuple ttb = new Tuple();
        try {
          ttb.setHdr((short) 4, qtypes, qsizes);
        }
        catch (Exception e) {
          status = FAIL;
          e.printStackTrace();
        }

        int sizett = ttb.size();
        ttb = new Tuple(sizett);
        try {
          ttb.setHdr((short) 4, qtypes, qsizes);
        }
        catch (Exception e) {
          status = FAIL;
          e.printStackTrace();
        }
        Heapfile        f = null;
        try {
          f = new Heapfile("q.in");
        }
        catch (Exception e) {
          status = FAIL;
          e.printStackTrace();
        }

        Scan scan = null;

        try {
          scan = new Scan(f);
        }
        catch (Exception e) {
          status = FAIL;
          e.printStackTrace();
          Runtime.getRuntime().exit(1);
        }

        // create the index file
        BTreeFile btf = null;
        try {
          btf = new BTreeFile("BTreeIndex", AttrType.attrInteger, 4, 1);
        }
        catch (Exception e) {
          status = FAIL;
          e.printStackTrace();
          Runtime.getRuntime().exit(1);
        }

        RID rid = new RID();
        int key =0;
        Tuple temp = null;

        try {
          temp = scan.getNext(rid);
        }
        catch (Exception e) {
          status = FAIL;
          e.printStackTrace();
        }
        while ( temp != null) {
          ttb.tupleCopy(temp);

          try {
    	key = ttb.getIntFld(1);
          }
          catch (Exception e) {
    	status = FAIL;
    	e.printStackTrace();
          }

          try {
    	btf.insert(new IntegerKey(key), rid);
          }
          catch (Exception e) {
    	status = FAIL;
    	e.printStackTrace();
          }

          try {
    	temp = scan.getNext(rid);
          }
          catch (Exception e) {
    	status = FAIL;
    	e.printStackTrace();
          }
        }

        // close the file scan
        scan.closescan();


        //_______________________________________________________________
        //*******************close an scan on the heapfile**************
        //++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

        System.out.print ("After Building btree index on R.3.\n\n");
        try {
          amb = new IndexScan ( bb_index, "q.in",
    			   "BTreeIndex", qtypes, qsizes, 4, 4,
    			   qprojection, null, 1, false);
          amb2 = new IndexScan ( bb_index, "q.in",
    			   "BTreeIndex", qtypes, qsizes, 4, 4,
    			   qprojection, null, 1, false);
        }

        catch (Exception e) {
          System.err.println ("*** Error creating scan for Index scan");
          System.err.println (""+e);
          Runtime.getRuntime().exit(1);
        }



                IESelfJoin2b all_results = null;
                long duration = 0;
                try {

                	long startTime = System.nanoTime();

                  all_results = new IESelfJoin2b (qtypes, 4, qsizes,
              				  10, amb, amb2, "q.in", outFilter, projectionfin, 2);

                  long endTime = System.nanoTime();

                  duration = (endTime - startTime)/1000000;

                  System.out.println("The IESelfjoin algorithm for query2b takes "+ duration +" milliseconds. \n");

                }
                catch (Exception e) {
                  System.err.println ("*** Error preparing for IESelfJoin2b");
                  System.err.println (""+e);
                  e.printStackTrace();
                  Runtime.getRuntime().exit(1);
                }

                try {
                    PrintWriter writer = new PrintWriter("../../../Output/output_tuples/output_query2b.txt", "UTF-8");
                    writer.println("**********************Query2b strating *********************\n");
                    writer.println ("  SELECT   R.col_" + selectLeft2b + ", S.col_" + selectRight2b + "\n"
              	        + "  FROM     R, S\n"
              	        + "  WHERE   R.col_" + condition1Left2b + " " + symbol12b.toSymbol() + " S.col_" + condition1Right2b
              	        + " " + rac2b + " R.col_" + condition2Left2b + " " + symbol22b.toSymbol() + " S.col_" + condition2Right2b );
                    writer.println("The NestedLoopJoin algorithm for query2b takes "+ duration +" nanoseconds.\n\n");
                    writer.println("Number of inputs : Q=" + numq + "\n\n");
                    writer.println("Here are the output tuples for query 2b : \n\n");

                    t = null;
                    int nbOut = 0;
                    try {
                      while ((t = all_results.get_next()) != null) {
                        t.print(Jtypes);
                        writer.println(t.getIntFld(1)+" "+t.getIntFld(2)+"\n");
                        nbOut = nbOut + 1;
                      }
                    }
                    catch (Exception e) {
                      System.err.println (""+e);
                      e.printStackTrace();
                      Runtime.getRuntime().exit(1);
                    }
                    writer.println("Number of outputs : " + nbOut + " tuples");
                    System.out.println("Number of outputs : " + nbOut + " tuples");

                    System.out.println("The IESelfjoin algorithm for query2b takes "+ duration +" milliseconds. \n");
                    writer.close();
                }
                catch (Exception e)
                {
                    System.out.println("Error when creating the file of output tuples");
                }

          System.out.println ("\n");
          try {
          	all_results.close();
          }
          catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
          }


          if (status != OK) {
            //bail out

            Runtime.getRuntime().exit(1);}
    }


    public void Query2c() {

    	AttrOperator symbol12c = new AttrOperator(op12c);
		AttrOperator symbol22c = new AttrOperator(op22c);

    	System.out.print("**********************Query2c strating *********************\n");
    	System.out.print("  SELECT   Q.col_" + selectLeft2c + ", Q.col_" + selectRight2c + "\n"
  	        + "  FROM     Q\n"
  	        + "  WHERE   Q.col_" + condition1Left2c + " " + symbol12c.toSymbol() + " Q.col_" + condition1Right2c
  	        + " " + rac2c + " Q.col_" + condition2Left2c + " " + symbol22c.toSymbol() + " Q.col_" + condition2Right2c );


        boolean status = OK;

        // Build Index first
        IndexType bb_index = new IndexType (IndexType.B_Index);
        IndexType bb_index2 = new IndexType (IndexType.B_Index);

        CondExpr [] outFilter = null;

    	if (numtxt2c == 5 && rac2c.compareTo("AND")==0) {
    		outFilter = new CondExpr[3];
            outFilter[0] = new CondExpr();
            outFilter[1] = new CondExpr();
            outFilter[2] = new CondExpr();
            Query2c_CondExpr(outFilter);
    	}
    	else if (numtxt2c == 5 && rac2c.compareTo("OR")==0) {
    		outFilter = new CondExpr[2];
            outFilter[0] = new CondExpr();
            outFilter[1] = new CondExpr();
            Query2c_CondExpr(outFilter);
    	}

    	else if (numtxt2c == 3) {
    		System.out.println("Please enter 2 conditions or verify that there is 5 lines in your query");
    	}

        Tuple t = new Tuple();
        t = null;









        // R table
        AttrType [] types2 = {
          new AttrType(AttrType.attrInteger),
          new AttrType(AttrType.attrInteger),
          new AttrType(AttrType.attrInteger),
          new AttrType(AttrType.attrInteger),
        };

        short  []  sizes2 = new short[1] ;
        sizes2[0] = 30;


        // S table
        AttrType [] types1 = {
          new AttrType(AttrType.attrInteger),
          new AttrType(AttrType.attrInteger),
          new AttrType(AttrType.attrInteger),
          new AttrType(AttrType.attrInteger)
        };

        short []   sizes1 = new short[1];
        sizes1[0] = 30;


        // Final table
        AttrType [] Jtypes = {
          new AttrType(AttrType.attrInteger),
          new AttrType(AttrType.attrInteger),
        };

        short []   Jsizesb = new short[1];
        Jsizesb[0] = 30;

        FldSpec [] Rprojectionb = {
        	       new FldSpec(new RelSpec(RelSpec.outer), 1),
        	       new FldSpec(new RelSpec(RelSpec.outer), 2),
        	       new FldSpec(new RelSpec(RelSpec.outer), 3),
        	       new FldSpec(new RelSpec(RelSpec.outer), 4),
        	    };


        FldSpec [] projectionfin = {
     	       new FldSpec(new RelSpec(RelSpec.outer), selectLeft2c),
     	       new FldSpec(new RelSpec(RelSpec.innerRel), selectRight2c),
     	    };


        CondExpr [] selects = new CondExpr[1];
        selects[0] = null;


        //IndexType b_index = new IndexType(IndexType.B_Index);
        iterator.Iterator amb1 = null;
        iterator.Iterator amb1bis = null;


        //_______________________________________________________________
        //*******************create an scan on the heapfile**************
        //+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
        // create a tuple of appropriate size
            Tuple ttb = new Tuple();
        try {
          ttb.setHdr((short) 4, types1, sizes1);
        }
        catch (Exception e) {
          status = FAIL;
          e.printStackTrace();
        }

        int sizett = ttb.size();
        ttb = new Tuple(sizett);
        try {
          ttb.setHdr((short) 4, types1, sizes1);
        }
        catch (Exception e) {
          status = FAIL;
          e.printStackTrace();
        }
        Heapfile        f = null;
        try {
          f = new Heapfile(nameBdd1+".in");
        }
        catch (Exception e) {
          status = FAIL;
          e.printStackTrace();
        }

        Scan scan = null;

        try {
          scan = new Scan(f);
        }
        catch (Exception e) {
          status = FAIL;
          e.printStackTrace();
          Runtime.getRuntime().exit(1);
        }

        // create the index file
        BTreeFile btf = null;
        try {
          btf = new BTreeFile("BTreeIndex", AttrType.attrInteger, 4, 1);
        }
        catch (Exception e) {
          status = FAIL;
          e.printStackTrace();
          Runtime.getRuntime().exit(1);
        }

        RID rid = new RID();
        int key =0;
        Tuple temp = null;

        try {
          temp = scan.getNext(rid);
        }
        catch (Exception e) {
          status = FAIL;
          e.printStackTrace();
        }
        while ( temp != null) {
          ttb.tupleCopy(temp);

          try {
    	key = ttb.getIntFld(1);
          }
          catch (Exception e) {
    	status = FAIL;
    	e.printStackTrace();
          }

          try {
    	btf.insert(new IntegerKey(key), rid);
          }
          catch (Exception e) {
    	status = FAIL;
    	e.printStackTrace();
          }

          try {
    	temp = scan.getNext(rid);
          }
          catch (Exception e) {
    	status = FAIL;
    	e.printStackTrace();
          }
        }

        // close the file scan
        scan.closescan();


        //_______________________________________________________________
        //*******************close an scan on the heapfile**************
        //++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

        System.out.print ("After Building btree index on R.3.\n\n");
        try {
          amb1 = new IndexScan ( bb_index, nameBdd1+".in",
    			   "BTreeIndex", types1, sizes1, 4, 4,
    			   Rprojectionb, null, 1, false);
          amb1bis = new IndexScan ( bb_index, nameBdd1+".in",
    			   "BTreeIndex", types1, sizes1, 4, 4,
    			   Rprojectionb, null, 1, false);
        }

        catch (Exception e) {
          System.err.println ("*** Error creating scan for Index scan");
          System.err.println (""+e);
          Runtime.getRuntime().exit(1);
        }













        //IndexType b_index = new IndexType(IndexType.B_Index);
        iterator.Iterator amb2 = null;
        iterator.Iterator amb2bis = null;


        //_______________________________________________________________
        //*******************create an scan on the heapfile**************
        //+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
        // create a tuple of appropriate size
            Tuple tt2 = new Tuple();
        try {
          tt2.setHdr((short) 4, types2, sizes2);
        }
        catch (Exception e) {
          status = FAIL;
          e.printStackTrace();
        }

        int sizett2 = tt2.size();
        tt2 = new Tuple(sizett2);
        try {
          tt2.setHdr((short) 4, types2, sizes2);
        }
        catch (Exception e) {
          status = FAIL;
          e.printStackTrace();
        }
        Heapfile        f2 = null;
        try {
          f2 = new Heapfile(nameBdd2+".in");
        }
        catch (Exception e) {
          status = FAIL;
          e.printStackTrace();
        }

        Scan scan2 = null;

        try {
          scan2 = new Scan(f);
        }
        catch (Exception e) {
          status = FAIL;
          e.printStackTrace();
          Runtime.getRuntime().exit(1);
        }

        // create the index file
        BTreeFile btf2 = null;
        try {
          btf2 = new BTreeFile("BTreeIndex", AttrType.attrInteger, 4, 1);
        }
        catch (Exception e) {
          status = FAIL;
          e.printStackTrace();
          Runtime.getRuntime().exit(1);
        }

        RID rid2 = new RID();
        int key2 =0;
        Tuple temp2 = null;

        try {
          temp2 = scan2.getNext(rid2);
        }
        catch (Exception e) {
          status = FAIL;
          e.printStackTrace();
        }
        while ( temp2 != null) {
          tt2.tupleCopy(temp2);

          try {
    	key2 = tt2.getIntFld(1);
          }
          catch (Exception e) {
    	status = FAIL;
    	e.printStackTrace();
          }

          try {
    	btf2.insert(new IntegerKey(key2), rid2);
          }
          catch (Exception e) {
    	status = FAIL;
    	e.printStackTrace();
          }

          try {
    	temp2 = scan2.getNext(rid2);
          }
          catch (Exception e) {
    	status = FAIL;
    	e.printStackTrace();
          }
        }

        // close the file scan
        scan2.closescan();


        //_______________________________________________________________
        //*******************close an scan on the heapfile**************
        //++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

        try {
          amb2 = new IndexScan ( bb_index2, nameBdd2+".in",
    			   "BTreeIndex", types2, sizes2, 4, 4,
    			   Rprojectionb, null, 1, false);
          amb2bis = new IndexScan ( bb_index2, nameBdd2+".in",
   			   "BTreeIndex", types2, sizes2, 4, 4,
   			   Rprojectionb, null, 1, false);
        }

        catch (Exception e) {
          System.err.println ("*** Error creating scan for Index scan");
          System.err.println (""+e);
          Runtime.getRuntime().exit(1);
        }

                IEJoin2c all_results = null;
                long duration = 0;

                try {

                long startTime = System.nanoTime();

                  all_results = new IEJoin2c (types1, 4, sizes1,
                		  types2, 4, sizes2,
              				  10, amb1, amb1bis, amb2, amb2bis, outFilter, projectionfin, 2);

                  long endTime = System.nanoTime();

                  duration = endTime - startTime;

                  System.out.println("The IESelfjoin algorithm for query2c takes "+ duration +" nanoseconds. \n");

                }
                catch (Exception e) {
                  System.err.println ("*** Error preparing for IEJoin2c");
                  System.err.println (""+e);
                  e.printStackTrace();
                  Runtime.getRuntime().exit(1);
                }

                try {
                    PrintWriter writer = new PrintWriter("../../../Output/output_tuples/output_query2c.txt", "UTF-8");
                    writer.println("**********************Query2c strating *********************\n");
                    writer.println ("  SELECT   Q.col_" + selectLeft2c + ", Q.col_" + selectRight2c + "\n"
              	        + "  FROM     Q\n"
              	        + "  WHERE   Q.col_" + condition1Left2c + " " + symbol12c.toSymbol() + " Q.col_" + condition1Right2c
              	        + " " + rac2c + " Q.col_" + condition2Left2c + " " + symbol22c.toSymbol() + " Q.col_" + condition2Right2c + "\n");
                    writer.println("The NestedLoopJoin algorithm for query2c takes "+ duration +" nanoseconds.\n\n");
                    writer.println("Number of inputs : R=" + numR +" et S=" + numS + "\n\n");
                    writer.println("Here are the output tuples for query 2c : \n\n");

                    t = null;
                    int nbOut = 0;
                    try {
                      while ((t = all_results.get_next()) != null) {
                        t.print(Jtypes);
                        writer.println(t.getIntFld(1)+" "+t.getIntFld(2)+"\n");
                        nbOut = nbOut + 1;
                      }
                    }
                    catch (Exception e) {
                      System.err.println (""+e);
                      e.printStackTrace();
                      Runtime.getRuntime().exit(1);
                    }
                    writer.println("Number of outputs : " + nbOut + " tuples");
                    writer.close();
                }
                catch (Exception e)
                {
                    System.out.println("Error when creating the file of output tuples");
                }

                t = null;
                try {
                  while ((t = all_results.get_next()) != null) {
                    t.print(Jtypes);
                  }
                }
                catch (Exception e) {
                    System.err.println (""+e);
                    e.printStackTrace();
                     Runtime.getRuntime().exit(1);
                  }


          System.out.println ("\n");
          try {
          	all_results.close();
          }
          catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
          }


          if (status != OK) {
            //bail out

            Runtime.getRuntime().exit(1);}
    }


  private void Disclaimer() {
	    System.out.print ("\n\nAny resemblance of persons in this database to"
	         + " people living or dead\nis purely coincidental. The contents of "
	         + "this database do not reflect\nthe views of the University,"
	         + " the Computer  Sciences Department or the\n"
	         + "developers...\n\n");
	  }

}

public class JoinTest
{
  public static void main(String argv[]) throws IOException
  {
    boolean sortstatus;
    //SystemDefs global = new SystemDefs("bingjiedb", 100, 70, null);
    //JavabaseDB.openDB("/tmp/nwangdb", 5000);

    JoinsDriver jjoin = new JoinsDriver();

    sortstatus = jjoin.runTests();
    if (sortstatus != true) {
      System.out.println("Error ocurred during join tests");
    }
    else {
      System.out.println("join tests completed successfully");
    }
  }
}
