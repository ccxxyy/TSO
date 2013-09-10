import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;
import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.LinkedHashSet;
import java.io.Reader;
import java.io.InputStreamReader;
import java.io.FileInputStream;
import java.io.ByteArrayInputStream;
import java.util.Properties;
import java.io.InputStream;
import java.util.Date;
import java.util.Map;
import java.util.HashMap;
import java.util.LinkedList;
import java.io.FileOutputStream;
import java.io.FileNotFoundException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Hashtable;
import java.io.Serializable;
import java.io.RandomAccessFile;
import java.io.File;
import java.util.Collections;
import java.util.logging.Handler;
import java.util.logging.Formatter;
import java.util.logging.Level;
import java.util.LinkedHashMap;
/*
 * Created on Dec 12, 2003 by sviglas
 *
 * This is part of the attica project. Any subsequent modification of
 * the file should retain this disclaimer.
 * 
 * University of Edinburgh, School of Informatics
 */

/**
 * AlgebraicOperator: The base class for all algebraic operators.
 *
 * @author sviglas
 */
abstract class AlgebraicOperator {

} // AlgebraicOperator
/*
 * Created on Dec 15, 2003 by sviglas
 *
 * Modified on Dec 26, 2008 by sviglas
 *
 * This is part of the attica project.  Any subsequent modification
 * of the file should retain this disclaimer.
 * 
 * University of Edinburgh, School of Informatics
 */


/**
 * InitialProjection: An initial projection -- used to project out
 * attributes before the final projection (this is a hack, really, but
 * it will do for now).
 *
 * @author sviglas
 */
class InitialProjection extends Projection {
	
    /**
     * Constructs a new initial projection algebraic operator.
     * 
     * @param list the list of attributes to be projected.
     */
    public InitialProjection(List<Variable> list) {
        super(list);
    } // InitialProjection()

    
    /**
     * A textual representation of this projection.
     * 
     * @return this projection's textual representation.
     */
    @Override
    public String toString () {
        return "[i-projection (" + getProjectionList().toString() + ")]";
    } // toString()


} // InitialProjection
/*
 * Created on Dec 12, 2003 by sviglas
 *
 * Modified on Dec 26, 2008 by sviglas
 *
 * This is part of the attica project.  Any subsequent modification
 * of the file should retain this disclaimer.
 * 
 * University of Edinburgh, School of Informatics
 */

/**
 * Join: An algebraic join operator.
 *
 * @author sviglas
 */
class Join extends AlgebraicOperator {
	
    /** This join's qualification. */
    private Qualification qualification;
	
    /** Default constructor. */
    public Join() {
        this(null);
    } // Join()

    
    /**
     * Constructs a new algebraic join operator.
     * 
     * @param qualification the qualification of this join.
     */
    public Join(Qualification qualification) {
        this.qualification = qualification;
    } // Join()

    
    /**
     * Return this join's qualification.
     * 
     * @return the qualification of this join.
     */
    public Qualification getQualification() {
        return qualification;
    } // getQualification()

    
    /**
     * A textual representation of this join.
     * 
     * @return this join's textual representation.
     */
    @Override
    public String toString() {
        return "[join (" + qualification.toString() + ")]";
    } // toString()
} // Join
/*
 * Created on Dec 12, 2003 by sviglas
 *
 * Modified on Dec 26, 2008 by sviglas
 *
 * This is part of the attica project.  Any subsequent modification
 * of the file should retain this disclaimer.
 * 
 * University of Edinburgh, School of Informatics
 */


/**
 * Projection: The algebraic representation of a projection.
 *
 * @author sviglas
 */
class Projection extends AlgebraicOperator {
	
    /** The projection list of this projection. */
    private List<Variable> projectionList;
	
    /**
     * Default constructor.
     */
    public Projection() {
        this(new ArrayList<Variable>());
    } // Projection()

    
    /**
     * Constructs a new projection.
     * 
     * @param projectionList the projection list of this projection.
     */
    public Projection(List<Variable> projectionList) {
        this.projectionList = projectionList;
    } // Projection()

    
    /**
     * Retrieves this projection's projection list.
     * 
     * @return the projection list of this projection.
     */
    public Iterable<Variable> projections() {
        return projectionList;
    } // getProjections()


    /**
     * Retrieves all projection variables.
     *
     * @return all projection variables.
     */
    public List<Variable> getProjectionList() {
        return projectionList;
    }

    
    /**
     * A textual representation of this projection.
     * 
     * @return this projection's textual representation
     */
    @Override
    public String toString() {
        return "[projection (" + projectionList.toString() + ")]";
    } // toString()

} // Projection
/*
 * Created on Dec 12, 2003 by sviglas
 *
 * Modified on Dec 26, 2008 by sviglas
 *
 * This is part of the attica project.  Any subsequent modification
 * of the file should retain this disclaimer.
 * 
 * University of Edinburgh, School of Informatics
 */

/**
 * Qualification: The base class of all algebraic qualifications.
 *
 * @author sviglas
 */
class Qualification {

    public enum Relationship {
	EQUALS, NOT_EQUALS, GREATER, LESS, GREATER_EQUALS, LESS_EQUALS
    }
	
    /** The relationship between the constituents of the
	qualification. */
    private Relationship relationship;
	
    /**
     * Default constructor.
     */
    public Qualification() {
        this(Relationship.EQUALS);
    } // Qualification()
	
    /**
     * Constructs a new qualification given a relationship.
     * 
     * @param relationship this qualification's relationship.
     */
    public Qualification(Relationship relationship) {
        this.relationship = relationship;
    } // Qualification()

    
    /**
     * Returns this qualification's relationship.
     * 
     * @return the relationship of the qualification.
     */
    public Relationship getRelationship() {
        return relationship;
    } // getRelationship()

    
    /**
     * Textual representation of this quelification.
     * 
     * @return this qualification's textual representation.
     */
    @Override
    public String toString() {
        switch (relationship) {
        case EQUALS:
            return "=";
        case GREATER:
            return ">";
        case LESS:
            return "<";
        case GREATER_EQUALS:
            return ">=";
        case LESS_EQUALS:
            return "<=";
        default:
            return "!=";
        }
    } // toString()
    
} // Qualification
/*
 * Created on Dec 12, 2003 by sviglas
 *
 * Modified on Dec 26, 2008 by sviglas
 *
 * This is part of the attica project.  Any subsequent modification
 * of the file should retain this disclaimer.
 * 
 * University of Edinburgh, School of Informatics
 */

/**
 * Selection: The algebraic representation of a selection.
 *
 * @author sviglas
 */
class Selection extends AlgebraicOperator {
	
    /** This selection's qualification. */
    private Qualification qualification;
    
    /**
     * Default constructor.
     */
    public Selection() {
        this(null);
    } // Selection()

    
    /**
     * Constructs a new algebraic selection operator.
     * 
     * @param qualification the qualification of this selection.
     */
    public Selection(Qualification qualification) {
        this.qualification = qualification;
    } // Selection()

    
    /**
     * Returns this selection's qualification.
     * 
     * @return the qualification of this selection.
     */
    public Qualification getQualification() {
        return qualification;
    } // getQualification()

    
    /**
     * A textual representation of this selection.
     * 
     * @return this selection's textual representation.
     */
    @Override
    public String toString() {
        return "[select (" + qualification.toString() + ")]";
    } // toString()

} // Selection
/*
 * Created on Jan 18, 2004 by sviglas
 *
 * Modified on Jan 26, 2009 by sviglas
 *
 * This is part of the attica project.  Any subsequent modification
 * of the file should retain this disclaimer.
 * 
 * University of Edinburgh, School of Informatics
 */


/**
 * @author sviglas
 *
 * Sort: Representation of a sort operation.
 */
class Sort extends AlgebraicOperator {
    /** The sort list of this operation. */
    private List<Variable> sortList;
	
    /**
     * Default constructor.
     */
    public Sort() {
        this(new ArrayList<Variable>());
    } // Sort()

    
    /**
     * Constructs a new sort operator.
     * 
     * @param sortList the sort list of this operation.
     */
    public Sort(List<Variable> sortList) {
        this.sortList = sortList;
    } // Sort()


    /**
     * Retrieves the sort's sort list.
     *
     * @return an iterable over the sort list.
     */
    public Iterable<Variable> sorts() {
        return sortList;
    }
	
    /**
     * Retrieves all sort variables.
     * 
     * @return the sort list of this operation.
     */
    public List<Variable> getSortList() {
        return sortList;
    } // getSortList()
	
    /**
     * A textual representation of this operation.
     * 
     * @return this operation's textual representation.
     */
    @Override
    public String toString() {
        return "[sort (" + sortList.toString() + ")]";
    } // toString()
} // Sort
/*
 * Created on Dec 12, 2003 by sviglas
 *
 * Modified on Dec 24, 2008 by sviglas
 *
 * This is part of the attica project.  Any subsequent modification
 * of the file should retain this disclaimer.
 * 
 * University of Edinburgh, School of Informatics
 */


/**
 * Variable: Encapsulates a qualification/projection variable.
 *
 * @author sviglas
 */
class Variable extends Pair<String, String> {
	
    /**
     * Constructs a new variable.
     * 
     * @param table the table of this variable.
     * @param attribute the attribute of this variable.
     */
    public Variable(String table, String attribute) {
        super(table, attribute);
    } // Variable()

    
    /**
     * Returns the table of this variable.
     * 
     * @return this variable's table.
     */
    public String getTable() {
        return first;
    } // getTable()

    
    /**
     * Returns the attribute of this variable.
     * 
     * @return this variable's attribute.
     */
    public String getAttribute() {
        return second;
    } // getAttribute()
	
} // Variable
/*
 * Created on Dec 12, 2003 by sviglas
 *
 * Modified on Dec 26, 2008 by sviglas
 *
 * This is part of the attica project.  Any subsequent modification
 * of the file should retain this disclaimer.
 * 
 * University of Edinburgh, School of Informatics
 */

/**
 * VariableValueQualification: A qualification between a variable and
 * a value.
 *
 * @author sviglas
 */
class VariableValueQualification extends Qualification {
	
    /** The variable of this qualification. */
    private Variable variable;
	
    /** The value of this qualification. */
    private String value;
	
    /**
     * Constructs a new qualification between a variable and a value.
     * 
     * @param relationship the relationship between the variable and
     * the value.
     * @param variable the variable of this qualification.
     * @param value the value of this qualification.
     */
    public VariableValueQualification(Relationship relationship,
                                      Variable variable,
                                      String value) {
        super(relationship);
        this.variable = variable;
        this.value = value;
    } // VariableValueQualification()

    
    /**
     * Returns this qualification's variable.
     * 
     * @return the variable of this qualification.
     */
    public Variable getVariable() {
        return variable;
    } // getVariable()

    
    /**
     * Return this qualification's value.
     * 
     * @return the value of this qualification.
     */
    public String getValue() {
        return value;
    } // getValue()
	
    /**
     * Textual representation.
     */
    @Override
    public String toString() {
        return variable.toString() + " " + super.toString() + " " + value;
    } // toString()

} // VariableValueQualification()
/*
 * Created on Dec 12, 2003 by sviglas
 *
 * Modified on Dec 26, 2008 by sviglas
 *
 * This is part of the attica project.  Any subsequent modification
 * of the file should retain this disclaimer.
 * 
 * University of Edinburgh, School of Informatics
 */

/**
 * VariableVariableQualification: A qualification between two
 * variables.
 *
 * @author sviglas
 */
class VariableVariableQualification extends Qualification {
	
    /** The left-hand side variable of the qualification. */
    private Variable left;
	
    /** The right-hand side variable of the qualification. */	
    private Variable right;
	
    /**
     * Constructs a new qualification between variables.
     * 
     * @param relationship the relationship of the qualification.
     * @param left the left-hand side variable of the qualification.
     * @param right the right-hand side variable of the qualification.
     */
    public VariableVariableQualification (Relationship relationship,
                                          Variable left,
                                          Variable right) {
        super(relationship);
        this.left = left;
        this.right = right;
    } // VariableVariableQualification()

    
    /**
     * Returns the left-hand side variable.
     * 
     * @return this qualification's left-hand side variable.
     */
    public Variable getLeftVariable() {
        return left;
    } // getLeftVariable()

    
    /**
     * Returns the right-hand side variable.
     * 
     * @return this qualification's right-hand side variable.
     */
    public Variable getRightVariable() {
        return right;
    } // getRightVariable()

    
    /**
     * Textual representation.
     */
    @Override
    public String toString() {
        return left.toString() + " " + super.toString() + 
            " " + right.toString();
    } // toString()

} // VariableVariableQualification
/*
 * Created on Dec 9, 2003 by sviglas
 *
 * Modified on Dec 24, 2008 by sviglas
 *
 * This is part of the attica project.  Any subsequenct modification
 * of the file should retain this disclaimer.
 * 
 * University of Edinburgh, School of Informatics
 */


/**
 * BinaryOperator: The basic binary operator class.
 *
 * @author sviglas
 */
abstract class BinaryOperator extends Operator {

    /** Denotes the left-hand side input operator. */
    // no need for an enumeration, this simply just references the
    // 0'th input of the operator
    public static final int LEFT = 0;
    
    /** Denotes the right-hand side input operator. */
    public static final int RIGHT = 1;

    
    /**
     * Constructs a new binary operator given a left and a right
     * input.
     * 
     * @param leftInput the left input operator to this binary
     * operator.
     * @param rightInput the right input operator to this binary
     * operator.
     * @throws EngineException thrown whenever the operator cannot be
     * properly constructed.
     */
    public BinaryOperator(Operator leftInput, Operator rightInput) 
	throws EngineException {

	super();
        List<Operator> inops = new ArrayList<Operator>();
        inops.add(leftInput);
        inops.add(rightInput);
        setInputs(inops);
    } // BinaryOperator()
    
} // BinaryOperator
/*
 * Created on Dec 14, 2003 by sviglas
 *
 * Modified on Dec 24, 2008 by sviglas
 *
 * This is part of the attica project.  Any subsequent modification
 * of the file should retain this disclaimer.
 * 
 * University of Edinburgh, School of Informatics
 */



/**
 * CartesianProduct: A cartesian product between two inuput operators.
 *
 * @author sviglas
 */
class CartesianProduct extends NestedLoopsJoin {
	
    /**
     * Constructs a new Cartesian product operator.
     * 
     * @param left the left input operator.
     * @param right the right input operator.
     * @param sm the storage manager.
     * @throws EngineException thrown whenever the operator cannot be 
     * properly constructed.
     */
    public CartesianProduct(Operator left, Operator right, StorageManager sm) 
	throws EngineException {
        
        super(left, right, sm, new TrueCondition());
    } // CartesianProduct()
	
} // CartesianProduct
/*
 * Created on Dec 8, 2003 by sviglas
 *
 * Modified on Dec 24, 2008 by sviglas
 *
 * This is part of the attica project.  Any subsequent modification
 * of the file should retain this disclaimer.
 * 
 * University of Edinburgh, School of Informatics
 */


/**
 * EndOfStreamTuple: Signifies the end of a stream.
 *
 * @author sviglas
 */
class EndOfStreamTuple extends Tuple {
	
    /**
     * Constructs a new end-of-stream tuple.
     */
    public EndOfStreamTuple() {
        super(null, null);
    } // EndOfStreamTuple()
    
    /**
     * Textual representation.
     */
    @Override
    public String toString() {
        return "** end-of-stream **";
    } // toString()

    @Override
    public String toStringFormatted() {
        return toString();
    } // toStringFormatted()
    
} // EndOfStreamTuple
/*
 * Created on Dec 8, 2003 by sviglas
 *
 * Modified on Dec 24, 2008 by sviglas
 *
 * This is part of the attica project.  Any subsequent modification
 * of the file should retain this disclaimer.
 * 
 * University of Edinburgh, School of Informatics
 */

/**
 * EngineException: Thrown whenever there is an engine error.
 *
 * @author sviglas
 */
class EngineException extends Exception {
	
    /**
     * Constructs a new exception.
     * 
     * @param message this exception's message.
     */
    public EngineException(String message) {
        super(message);
    } // EngineException()

    /**
     * Constructs a new exception given the error message and
     * originating throwable.
     *
     * @param msg the error message.
     * @param e the originating throwable.
     */
    public EngineException(String msg, Throwable e) {
        super(msg, e);
    } // EngineException()

} // EngineException
/*
 * Created on Jan 18, 2004 by sviglas
 *
 * Modified on Dec 24, 2008 by sviglas
 *
 * This is part of the attica project.  Any subsequent modification
 * of the file should retain this disclaimer.
 * 
 * University of Edinburgh, School of Informatics
 */
//package attica.src.org.dejave.attica.engine.operators;
//
//import java.util.List;
//import java.util.ArrayList;
//import java.util.Iterator;
//
//import java.io.IOException;
//
//import attica.src.org.dejave.attica.model.Relation;
//
//import attica.src.org.dejave.attica.engine.predicates.Predicate;
//import attica.src.org.dejave.attica.engine.predicates.PredicateEvaluator;
//import attica.src.org.dejave.attica.engine.predicates.PredicateTupleInserter;
//
//import attica.src.org.dejave.attica.storage.IntermediateTupleIdentifier;
//import attica.src.org.dejave.attica.storage.RelationIOManager;
//import attica.src.org.dejave.attica.storage.StorageManager;
//import attica.src.org.dejave.attica.storage.BufferManager;
//import attica.src.org.dejave.attica.storage.StorageManagerException;
//import attica.src.org.dejave.attica.storage.Tuple;
//import attica.src.org.dejave.attica.storage.Page;
//import attica.src.org.dejave.attica.storage.PageIdentifier;
//
//import attica.src.org.dejave.attica.storage.FileUtil;







/**
 * ExternalSort: Your implementation of sorting.
 *
 * @author sviglas
 */
class ExternalSort extends UnaryOperator {
    
    /** The storage manager for this operator. */
    private StorageManager sm;
    
    /** The name of the temporary file for the output. */
    private String outputFile;
	
    /** The manager that undertakes output relation I/O. */
    private RelationIOManager outputMan;
	
    /** The slots that act as the sort keys. */
    private int [] slots;
	
    /** Number of buffers (i.e., buffer pool pages and 
     * output files). */
    private int buffers;

    /** Iterator over the output file. */
    private Iterator<Tuple> outputTuples;

    /** Reusable tuple list for returns. */
    private List<Tuple> returnList;

	private String tempFile;
	
    
    /**
     * Constructs a new external sort operator.
     * 
     * @param operator the input operator.
     * @param sm the storage manager.
     * @param slots the indexes of the sort keys.
     * @param buffers the number of buffers (i.e., run files) to be
     * used for the sort.
     * @throws EngineException thrown whenever the sort operator
     * cannot be properly initialized.
     */
    public ExternalSort(Operator operator, StorageManager sm,
                        int [] slots, int buffers) 
	throws EngineException {
        
        super(operator);
        this.sm = sm;
        this.slots = slots;
        this.buffers = buffers;
        try {
            // create the temporary output files
            initTempFiles();
        }
        catch (StorageManagerException sme) {
            throw new EngineException("Could not instantiate external sort",
                                      sme);
        }
    } // ExternalSort()
	

    /**
     * Initialises the temporary files, according to the number
     * of buffers.
     * 
     * @throws StorageManagerException thrown whenever the temporary
     * files cannot be initialised.
     */
    protected void initTempFiles() throws StorageManagerException {
        ////////////////////////////////////////////
        //
        // initialise the temporary files here
        // make sure you throw the right exception
        // in the event of an error
        //
        // for the time being, the only file we
        // know of is the output file
        //
        ////////////////////////////////////////////
    	
    	//initiate ouput file
    	outputFile = FileUtil.createTempFileName();
        sm.createFile(outputFile);
        //initiate temp file to store tuple
        tempFile = FileUtil.createTempFileName();
        sm.createFile(tempFile);
    } // initTempFiles()

    
    
    @SuppressWarnings("unchecked")
	public int tupleCompare (Tuple t1, Tuple t2){
    	for (int i = 0; i < slots.length; i++) {
	        int comp = (t1.getValue(slots[i])).compareTo(t2.getValue(slots[i]));
	        if (comp != 0) return comp;
	    }
	    return 0;
    }
    
    /**
     * Sets up this external sort operator.
     * 
     * @throws EngineException thrown whenever there is something wrong with
     * setting this operator up
     */
    public void setup() throws EngineException {
        returnList = new ArrayList<Tuple>();
        try {
            ////////////////////////////////////////////
            //
            // this is a blocking operator -- store the input
            // in a temporary file and sort the file
            //
            ////////////////////////////////////////////
            
            ////////////////////////////////////////////
            //
            // YOUR CODE GOES HERE
            //
            ////////////////////////////////////////////
        	//System.out.println("ffffffffffff");
        	Relation rel = getInputOperator().getOutputRelation();
        	
        	RelationIOManager tempFileManager = new RelationIOManager(sm, rel, tempFile);
        	
        	while(1==1){
        		Tuple tuple = getInputOperator().getNext();
        		if((tuple instanceof EndOfStreamTuple)==true){
        			break;
        		}
        		//Tuple tuple = getInputOperator().getNext();
        		tempFileManager.insertTuple(tuple);
        			
        	}
        	
 
            for (Page page : tempFileManager.pages()) {           	
            	
        		int NoOfTuples = page.getNumberOfTuples();

        		for (int i = 0 ; i<NoOfTuples ; i++) {
        			Tuple top = page.retrieveTuple(i);
        			int position = i;
        			for (int j = i ; j<NoOfTuples ; j++) {
        				Tuple temp_tuple = page.retrieveTuple(j);
        				if (tupleCompare (top, temp_tuple) < 0) {
        					top = temp_tuple;
        					position = j;
        					//break;
        				}
        			}
        			page.swap(i, position);
        		}
            }
         
            System.out.println("aaaaaaaaaaaaaaabbbbbbbb");
            ////////////////////////////////////////////
            //
            // the output should reside in the output file
            //
            ////////////////////////////////////////////
            
            outputMan = new RelationIOManager(sm, getOutputRelation(),
                                              outputFile);
            
            for (Tuple tuple : tempFileManager.tuples()) {
            	outputMan.insertTuple(tuple);
            	}
            //System.out.println("aaaaaaaaaaaaaaacccccccccccc");
            outputTuples = outputMan.tuples().iterator();
        }
        catch (Exception sme) {
            throw new EngineException("Could not store and sort"
                                      + "intermediate files.", sme);
        }
    } // setup()

    
    /**
     * Cleanup after the sort.
     * 
     * @throws EngineException whenever the operator cannot clean up
     * after itself.
     */
    public void cleanup () throws EngineException {
        try {
            ////////////////////////////////////////////
            //
            // make sure you delete the intermediate
            // files after sorting is done
            //
            ////////////////////////////////////////////
            
            ////////////////////////////////////////////
            //
            // right now, only the output file is 
            // deleted
            //
            ////////////////////////////////////////////
            sm.deleteFile(outputFile);
            sm.deleteFile(tempFile);
        }
        catch (StorageManagerException sme) {
            throw new EngineException("Could not clean up final output.", sme);
        }
    } // cleanup()

    
    /**
     * The inner method to retrieve tuples.
     * 
     * @return the newly retrieved tuples.
     * @throws EngineException thrown whenever the next iteration is not 
     * possible.
     */    
    protected List<Tuple> innerGetNext () throws EngineException {
        try {
            returnList.clear();
            if (outputTuples.hasNext()) returnList.add(outputTuples.next());
            else returnList.add(new EndOfStreamTuple());
            return returnList;
        }
        catch (Exception sme) {
            throw new EngineException("Could not read tuples " +
                                      "from intermediate file.", sme);
        }
    } // innerGetNext()


    /**
     * Operator class abstract interface -- never called.
     */
    protected List<Tuple> innerProcessTuple(Tuple tuple, int inOp)
	throws EngineException {
        return new ArrayList<Tuple>();
    } // innerProcessTuple()

    
    /**
     * Operator class abstract interface -- sets the ouput relation of
     * this sort operator.
     * 
     * @return this operator's output relation.
     * @throws EngineException whenever the output relation of this
     * operator cannot be set.
     */
    protected Relation setOutputRelation() throws EngineException {
        return new Relation(getInputOperator().getOutputRelation());
    } // setOutputRelation()

} // ExternalSort
/*
 * Created on Feb 11, 2004 by sviglas
 *
 * Modified on Feb 17, 2009 by sviglas
 *
 * This is part of the attica project.  Any subsequent modification
 * of the file should retain this disclaimer.
 * 
 * University of Edinburgh, School of Informatics
 */







/**
 * MergeJoin: Implements a merge join. The assumptions are that the
 * input is already sorted on the join attributes and the join being
 * evaluated is an equi-join.
 *
 * @author sviglas
 * 
 */
class MergeJoin extends NestedLoopsJoin {
	
    /** The name of the temporary file for the output. */
    private String outputFile;
    
    /** The relation manager used for I/O. */
    private RelationIOManager outputMan;
    
    /** The pointer to the left sort attribute. */
    private int leftSlot;
	
    /** The pointer to the right sort attribute. */
    private int rightSlot;

    /** The iterator over the output file. */
    private Iterator<Tuple> outputTuples;

    /** Reusable output list. */
    private List<Tuple> returnList;
	
    /**
     * Constructs a new mergejoin operator.
     * 
     * @param left the left input operator.
     * @param right the right input operator.
     * @param sm the storage manager.
     * @param leftSlot pointer to the left sort attribute.
     * @param rightSlot pointer to the right sort attribute.
     * @param predicate the predicate evaluated by this join operator.
     * @throws EngineException thrown whenever the operator cannot be
     * properly constructed.
     */
    public MergeJoin(Operator left, 
                     Operator right,
                     StorageManager sm,
                     int leftSlot,
                     int rightSlot,
                     Predicate predicate) 
	throws EngineException {
        
        super(left, right, sm, predicate);
        this.leftSlot = leftSlot;
        this.rightSlot = rightSlot;
        returnList = new ArrayList<Tuple>(); 
        try {
            initTempFiles();
        }
        catch (StorageManagerException sme) {
            EngineException ee = new EngineException("Could not instantiate " +
                                                     "merge join");
            ee.setStackTrace(sme.getStackTrace());
            throw ee;
        }
    } // MergeJoin()


    /**
     * Initialise the temporary files -- if necessary.
     * 
     * @throws StorageManagerException thrown whenever the temporary
     * files cannot be initialised.
     */
    protected void initTempFiles() throws StorageManagerException {
        ////////////////////////////////////////////
        //
        // initialise the temporary files here
        // make sure you throw the right exception
        //
        ////////////////////////////////////////////
        outputFile = FileUtil.createTempFileName();
    } // initTempFiles()

    
    /**
     * Sets up this merge join operator.
     * 
     * @throws EngineException thrown whenever there is something
     * wrong with setting this operator up.
     */
    
    @Override
    protected void setup() throws EngineException {
        try {
            System.out.println("done");
            ////////////////////////////////////////////
            //
            // YOUR CODE GOES HERE
            //
            ////////////////////////////////////////////
            
            ////////////////////////////////////////////
            //
            // the output should reside in the output file
            //
            ////////////////////////////////////////////

            //
            // you may need to uncomment the following lines if you
            // have not already instantiated the manager -- it all
            // depends on how you have implemented the operator
            //
            //outputMan = new RelationIOManager(getStorageManager(), 
            //                                  getOutputRelation(),
            //                                  outputFile);

            // open the iterator over the output
            outputTuples = outputMan.tuples().iterator();
        }
        catch (IOException ioe) {
            throw new EngineException("Could not create page/tuple iterators.",
                                      ioe);
        }
        catch (StorageManagerException sme) {
            EngineException ee = new EngineException("Could not store " + 
                                                     "intermediate relations " +
                                                     "to files.");
            ee.setStackTrace(sme.getStackTrace());
            throw ee;
        }
    } // setup()
    
    
    /**
     * Cleans up after the join.
     * 
     * @throws EngineException whenever the operator cannot clean up
     * after itself.
     */
    /*
    @Override
    protected void cleanup() throws EngineException {
        try {
            ////////////////////////////////////////////
            //
            // make sure you delete any temporary files
            //
            ////////////////////////////////////////////
            
            getStorageManager().deleteFile(outputFile);
        }
        catch (StorageManagerException sme) {
            EngineException ee = new EngineException("Could not clean up " +
                                                     "final output");
            ee.setStackTrace(sme.getStackTrace());
            throw ee;
        }
    } // cleanup()
    */

    /**
     * Inner method to propagate a tuple.
     * 
     * @return an array of resulting tuples.
     * @throws EngineException thrown whenever there is an error in
     * execution.
     */
    @Override
    protected List<Tuple> innerGetNext () throws EngineException {
        try {
            returnList.clear();
            if (outputTuples.hasNext()) returnList.add(outputTuples.next());
            else returnList.add(new EndOfStreamTuple());
            return returnList;
        }
        catch (Exception sme) {
            throw new EngineException("Could not read tuples "
                                      + "from intermediate file.", sme);
        }
    } // innerGetNext()


    /**
     * Inner tuple processing.  Returns an empty list but if all goes
     * well it should never be called.  It's only there for safety in
     * case things really go badly wrong and I've messed things up in
     * the rewrite.
     */
    @Override
    protected List<Tuple> innerProcessTuple(Tuple tuple, int inOp)
	throws EngineException {
        
        return new ArrayList<Tuple>();
    }  // innerProcessTuple()

    
    /**
     * Textual representation
     */
    protected String toStringSingle () {
        return "mj <" + getPredicate() + ">";
    } // toStringSingle()

} // MergeJoin
/*
 * Created on Dec 24, 2003 by sviglas
 *
 * Modified on Dec 24, 2008 by sviglas
 *
 * This is part of the attica project.  Any subsequent modification
 * of the file should retain this disclaimer.
 * 
 * University of Edinburgh, School of Informatics
 */




/**
 * MessageSink: A simple interface to provide DB messages to the user
 * as an operator.
 *
 * @author sviglas
 */
class MessageSink extends Sink {
	
    /** This sink's message. */
    private String message;

    /** Reusable return list. */
    private List<Tuple> returnList;
	
    /**
     * Constructs a new message sink.
     * 
     * @param message the message.
     * @throws EngineException thrown whenever the operator cannot
     * be properly constructed.
     */
    public MessageSink(String message) throws EngineException {
        super();
        this.message = message;
        returnList = new ArrayList<Tuple>();
    } // MessageSink()

    
    /**
     * Overriden method to retrieve this sink's message.
     * 
     * @return the tuple list containing the message
     * @throws EngineException thrown whenever the message cannot be retrieved
     */
    @Override
    public List<Tuple> getMultiNext() throws EngineException {
        returnList.clear();
        TupleIdentifier tid = new TupleIdentifier("DBResult", 0);
        List<Comparable> v = new ArrayList<Comparable>();
        v.add(message);
        Tuple tuple = new Tuple(tid, v);
        returnList.add(tuple);
        returnList.add(new EndOfStreamTuple());
        return returnList;
    } // getNext()

    
    /**
     * The inner tuple processing method -- doesn't do anything.
     * 
     * @param tuple the tuple to be processed.
     * @param inOp the index of the input operator the tuple to be
     * processed belongs to.
     * @return the resulting tuples
     * @throws EngineException thrown whenever there is something
     * wrong with processing the tuple.
     */
    protected List<Tuple> innerProcessTuple(Tuple tuple, int inOp)
	throws EngineException {
        return new ArrayList<Tuple>();
    } // innerProcessTuple()

    
    /**
     * Doesn't do anything -- returns null by default.
     * 
     * @return a new output relation.
     * @throws EngineException whenever an output relation cannot be
     * constructed.
     */
    protected Relation setOutputRelation() throws EngineException {
        return null;
    } // setOutputRelation()
    
} // MessageSink
/*
 * Created on Dec 12, 2003 by sviglas
 *
 * Modified on Dec 24, 2008 by sviglas
 *
 * This is part of the attica project.  Any subsequent modification
 * of the file should retain this disclaimer.
 * 
 * University of Edinburgh, School of Informatics
 */







/**
 * NestedLoopsJoin: Implements a (blocking) nested loops join.
 *
 * @author sviglas
 */
class NestedLoopsJoin extends PhysicalJoin {

    /** The name of the temporary file for the left input. */
    private String leftFile;
    
    /** The name of the temporary file for the right input. */
    private String rightFile;
	
    /** The name of the temporary file for the output. */
    private String outputFile;
	
    /** The relation manager used for I/O. */
    private RelationIOManager outputMan;

    /** The iterator over the output file. */
    private Iterator<Tuple> outputTuples;

    /** Reusable output list. */
    private List<Tuple> returnList;
	
    /**
     * Constructs a new nested loops join operator.
     * 
     * @param left the left input operator.
     * @param right the right input operator.
     * @param sm the storage manager.
     * @param predicate the predicate evaluated by this join operator.
     * @throws EngineException thrown whenever the operator cannot be 
     * properly constructed.
     */
    public NestedLoopsJoin (Operator left, Operator right,
                            StorageManager sm, Predicate predicate) 
	throws EngineException {
        
        super(left, right, sm, predicate);
        try {
            leftFile = FileUtil.createTempFileName();
            sm.createFile(leftFile);
            rightFile = FileUtil.createTempFileName();
            sm.createFile(rightFile);
            returnList = new ArrayList<Tuple>();            
        }
        catch (StorageManagerException sme) {
            throw new EngineException("Could not instantiate "
                                      + "nested-loops join", sme);
        }
    } // NestedLoopsJoin()

    
    /**
     * Sets up a nested loops join operator.
     * 
     * @throws EngineException thrown whenever there is something
     * wrong with setting this operator up.
     */
    @Override
    protected void setup() throws EngineException {
        try {
            // this is a blocking operator -- store the input
            // in temporary files
            
            // store the left input
            Relation leftRel = getInputOperator(LEFT).getOutputRelation();
            RelationIOManager leftMan =
                new RelationIOManager(getStorageManager(), leftRel, leftFile);
            boolean done = false;
            while (! done) {
                Tuple tuple = getInputOperator(LEFT).getNext();
                if (tuple != null) {
                    done = (tuple instanceof EndOfStreamTuple);
                    if (! done) leftMan.insertTuple(tuple);
                }
            }
            
            // store the right input
            Relation rightRel = getInputOperator(RIGHT).getOutputRelation();
            RelationIOManager rightMan = 
                new RelationIOManager(getStorageManager(), rightRel, rightFile);
            done = false;
            while (! done) {
                Tuple tuple = getInputOperator(RIGHT).getNext();
                if (tuple != null) {
                    done = (tuple instanceof EndOfStreamTuple);
                    if (! done) rightMan.insertTuple(tuple);
                }
            }
            
            // the inputs are now stored -- perform the nested loops join
            outputFile = FileUtil.createTempFileName();
            getStorageManager().createFile(outputFile);
            outputMan = new RelationIOManager(getStorageManager(), 
                                              getOutputRelation(), 
                                              outputFile);
            for (Tuple leftTuple : leftMan.tuples()) {
                for (Tuple rightTuple : rightMan.tuples()) {
                    PredicateTupleInserter.insertTuples(leftTuple, 
                                                        rightTuple,
                                                        getPredicate());
                    if (PredicateEvaluator.evaluate(getPredicate())) {
                        // the predicate is true -- store the new tuple
                        Tuple newTuple = combineTuples(leftTuple, rightTuple);
                        outputMan.insertTuple(newTuple);
                    }
                }
            }
            
            //outputMan = new RelationIOManager(getStorageManager(),
            //                                  getOutputRelation(),
            //                                  outputFile);
            // now open the iterator over the output
            outputTuples = outputMan.tuples().iterator();
        }
        catch (IOException ioe) {
            throw new EngineException("Could not create page/tuple iterators.",
                                      ioe);
        }
        catch (StorageManagerException sme) {
            throw new EngineException("Could not store intermediate relations " + 
                                      "to files.", sme);
        }
    } // setup()

    
    /**
     * Cleanup after the join.
     * 
     * @throws EngineException whenever the operator cannot clean up
     * after itself.
     */
    @Override
    protected void cleanup() throws EngineException {
        try {
            getStorageManager().deleteFile(leftFile);
            getStorageManager().deleteFile(rightFile);
            getStorageManager().deleteFile(outputFile);
        }
        catch (StorageManagerException sme) {
            throw new EngineException("Could not clean up final output", sme);
        }
    } // cleanup()
	

    /**
     * Given two tuples, combine them into a single one.
     * 
     * @param left the left tuple.
     * @param right the right tuple.
     * @return a new tuple with the left and right tuples combined.
     */    
    protected Tuple combineTuples(Tuple left, Tuple right) {
        List<Comparable> v = new ArrayList<Comparable>();
        v.addAll(left.getValues());
        v.addAll(right.getValues());
        return new Tuple(new IntermediateTupleIdentifier(tupleCounter++), v);
    } // combineTuples()

    
    /**
     * Inner method to propagate a tuple.
     * 
     * @return an array of resulting tuples.
     * @throws EngineException thrown whenever there is an error in
     * execution.
     */
    @Override
    protected List<Tuple> innerGetNext () throws EngineException {
        try {
            returnList.clear();
            if (outputTuples.hasNext()) returnList.add(outputTuples.next());
            else returnList.add(new EndOfStreamTuple());
            return returnList;
        }
        catch (Exception sme) {
            throw new EngineException("Could not read tuples "
                                      + "from intermediate file.", sme);
        }
    } // innerGetNext()
        

    @Override
    protected List<Tuple> innerProcessTuple(Tuple tuple, int inOp)
	throws EngineException {
        
        return new ArrayList<Tuple>();
    }  // innerProcessTuple()

    
    /**
     * Textual representation.
     */
    @Override
    protected String toStringSingle() {
        return "nlj <" + getPredicate() + ">";
    } // toStringSingle()

} // NestedLoopsJoin
/*
 * Created on Jan 18, 2004 by sviglas
 *
 * Modified on Jan 26, 2009 by sviglas
 *
 * This is part of the attica project.  Any subsequent modification
 * of the file should retain this disclaimer.
 * 
 * University of Edinburgh, School of Informatics
 */


/**
 * @author sviglas
 *
 * NullUnaryOperator: The null unary operator -- 
 * a select with a true predicate
 */
class NullUnaryOperator extends Select {
	
    /**
     * Constructs a new null operator.
     * 
     * @param operator the incoming operator.
     * @throws EngineException thrown whenever the operator cannot be
     * properly initialised.
     */
    public NullUnaryOperator(Operator operator) throws EngineException {
        super(operator, new TrueCondition());
    } // NullUnaryOperator()

} // NullUnaryOperator
/*
 * Created on Dec 8, 2003 by sviglas
 *
 * This is part of the attica project.  Any subsequent modification
 * of the file should retain this disclaimer.
 * 
 * University of Edinburgh, School of Informatics
 */



/**
 * Operator: The basic operator abstraction. It implements all the
 * plubming for subsequent concrete operators. It is more general than
 * it need be, in the sense that it handles multi-input operators
 * (when in reality most operators will be unary or binary) and is
 * capable of producing multiple outputs with a single call. This can
 * be confusing at times, but, when extending it, it makes it much
 * easier to cater more execution models than a standard push/pull one
 * (e.g., symmetric hash join can be easily implemented since there is
 * a way to return multiple outputs per getNext() call.)
 *
 * Luckily, all this is masked to the programmer. The operator itself
 * buffers all multiple possible outputs and returns a single tuple to
 * the caller through a getNext() call.
 *
 * @author sviglas
 */
abstract class Operator {
	
    /** A counter of already produced tuples -- all subclasses have access
     * to this counter. */
    protected int tupleCounter;
    
    /** Index of exhausted inputs. */
    private boolean [] done;
	
    /** The incoming operators. */
    private List<Operator> inOps;
	
    /** The number of incoming operators. */
    private int numInOps;
	
    /** This operator's output relation. */
    private Relation relation;
	
    /** Is this the first call to getNext() or not? */
    private boolean firstGetNext;

    private List<Tuple> buffer;
    private int bufferIndex;
    private boolean fromBuffer;
		
    /**
     * Default constructor.
     * 
     * @throws EngineException thrown whenever the operator cannot be
     * initialised.
     */
    public Operator() throws EngineException {
        this(new ArrayList<Operator>());
    } // Operator()

    
    /**
     * Constructs a new operator given the input operators.
     * 
     * @param inOps the list of input operators.
     * @throws EngineException thrown whenever the operator cannot be
     * initialised.
     */
    public Operator(List<Operator> inOps) throws EngineException {
        tupleCounter = 0;
        //relation = setOutputRelation();
        relation = null;
        firstGetNext = true;
        fromBuffer = false;
        bufferIndex = 0;
        setInputs(inOps);
    } // Operator()

    
    /**
     * Returns the number of inputs of this operator.
     * 
     * @return this operator's number of inputs.
     */
    public int getNumberOfInputs() {
        return numInOps;
    } // getNumerOfInputs()

    
    /**
     * Inner method to set the inputs of the operator.
     * 
     * @param inOps the input operators.
     */
    protected final void setInputs(List<Operator> inOps) {
        this.inOps = inOps;
        this.numInOps = inOps.size();
        done = new boolean [numInOps];
        for (boolean d : done) d = false;
    } // setInputs()

    
    /**
     * Retrieve the specified input operator.
     * 
     * @param which the index of the operator that should be retrieved
     * @return the specified input operator
     */
    public Operator getInputOperator(int which) throws EngineException {
        try {
            return inOps.get(which);
        }
        catch (ArrayIndexOutOfBoundsException aibe) {
            throw new EngineException("Retrieving a non-existing "
                                      + "input operator.", aibe);
        }
    } // getInputOperator()

    
    /**
     * Returns this operator's output relation.
     * 
     * @return this operator's output relation.
     */
    public Relation getOutputRelation() throws EngineException {
        if (relation == null) {
            relation = setOutputRelation();
        }
        return relation;
    } // getOutputRelation()

    
    /**
     * Some operators might need to perform some initial setting up.
     * This method is a placeholder for those operators.  By default
     * it does nothing.
     * 
     * @throws EngineException thrown whenever there is something wrong
     * during setup.
     */
    protected void setup() throws EngineException {
        // default implementation is a no-op
    } // setup()


    /**
     * Wrapper for the multi-output get next method.  In all
     * likelyhood, this is the one that will be called throughout,
     * unless the model is to be substantially extended.
     *
     * @return the next output tuple of this operator.
     * @throws EngineException whenever the next tuple cannot be
     * retrieved.
     */
    public Tuple getNext() throws EngineException {
        // first check if we're not reading from a buffer
        if (!fromBuffer) {
            // retrieve a buffer
            buffer = getMultiNext();
            // if there's nothing there, bail out; otherwise, start
            // scanning the buffer
            if (buffer == null || buffer.size() == 0) return null;
            else bufferIndex = 0;
        }

        // definitely in a buffer, so get the current tuple and set up
        // the next call
        Tuple t = buffer.get(bufferIndex++);
        fromBuffer = bufferIndex < buffer.size();
        return t;
    } // getNext()

    
    /**
     * Fetch the next tuple(s) from this operator.
     * 
     * @return the next tuples for this operator.
     * @throws EngineException thrown whenever there is something
     * wrong with retrieving tuples from this operator.
     */	
    public List<Tuple> getMultiNext() throws EngineException {
        // call setup the first time the operator is called -- NB: we
        // could not have called that during construction, because all
        // resources might not have been available then
        if (firstGetNext) {
            setup();
            firstGetNext = false;
        }

        // keep looping till we have a non-empty result
        List<Tuple> t = innerGetNext();
        while (t.size() == 0) t = innerGetNext();
        // clean up if we've seen the end
        if (t.size() == 1 && t.get(0) instanceof EndOfStreamTuple) cleanup();
        
        return t;
    } // getNext()
    
	
    /**
     * Called to clean up any resources held by the operator.  Default
     * implementation does nothing.
     * 
     * @throws EngineException thrown whenever there is something wrong 
     * during cleanup.
     */
    protected void cleanup() throws EngineException {
        // default implementation is a no-op
    } // cleanup()

    
    /**
     * Inner method to fetch the next tuple(s) from this operator.
     * The data flow of the standard implementation is to loop over
     * all non-exhausted inputs, retrieve tuples, process them, and put
     * them in an output list, which will be returned to the caller.
     * 
     * @return the next tuples for this operator.
     * @throws EngineException thrown whenever there is something
     * wrong with retrieving tuples from this operator.
     */
    protected List<Tuple> innerGetNext() throws EngineException {
        List<Tuple> outgoing = new ArrayList<Tuple>();

        int i = 0;
        for (Operator operator : inOps) {
            if (! done[i]) {
                List<Tuple> inTuples = operator.getMultiNext();
                for (Tuple tuple : inTuples) {
                    List<Tuple> temp = processTuple(tuple, i);
                    outgoing.addAll(temp);
                }
            }
            i++;
        }
        return outgoing;
    } // getNext()
	

    /**
     * Processes a single tuple from an operator, producing multiple
     * tuples in the output.
     * 
     * @param tuple the tuple to be processed.
     * @param inStream the index of the input operator this tuple
     * belongs to.
     * @return a list of output tuples.
     * @throws EngineException whenever there is an error while
     * processing the tuple.
     */
    protected List<Tuple> processTuple(Tuple tuple, int inStream) 
	throws EngineException {
        
        // first check whether this is an end of stream tuple
        if (tuple instanceof EndOfStreamTuple) {
            done[inStream] = true;
            List<Tuple> tuples = new ArrayList<Tuple>();
            if (allDone()) tuples.add(new EndOfStreamTuple());
            return tuples;
        }
        else {
            return innerProcessTuple(tuple, inStream);
        }
    } // processTuple()

    
    /**
     * Are all the incoming operators done or not?
     * 
     * @return <code>true</code> if all incoming operators are done, 
     * <code>false</code> otherwise.
     */
    protected boolean allDone() {
        for (boolean d : done) if (! d) return false;
        return true;
    } // allDone()

    
    /**
     * The inner tuple processing method -- all operators should implement 
     * this.
     * 
     * @param tuple the tuple to be processed.
     * @param inOp the index of the input operator the tuple to be
     * processed belongs to.
     * @return the resulting tuples.
     * @throws EngineException thrown whenever there is something wrong
     * with processing the tuple.
     */
    protected abstract List<Tuple> innerProcessTuple(Tuple tuple, int inOp)
	throws EngineException;
	
    /**
     * Subclasses should implement this method in order to specify the
     * output relation of the operator.
     * 
     * @return a new output relation.
     * @throws EngineException whenever an output relation cannot be 
     * constructed.
     */
    protected abstract Relation setOutputRelation() throws EngineException;
	
    /**
     * Textual representation.
     *
     * @return the textual representation of this operator.
     */
    @Override
    public String toString () {
        return toString(0);
    } // toString()

    
    /**
     * Convert only this operator to string and not the ones below.
     * 
     * @return the textual representation of this single operator.
     */
    protected String toStringSingle() {
        return "operator";
    } // toStringSingle()

    
    /**
     * Textual representation starting at a given level of the evaluation
     * plan.
     * 
     * @param level the level of the operator.
     * @return the textual representation of the operator.
     */
    public String toString(int level) {
        String prefix = prefix(level);
        StringBuffer sb = new StringBuffer();
        sb.append(prefix);
        sb.append(toStringSingle() + "\n");
        for (Operator operator : inOps)
            sb.append(operator.toString(level+1) + "\n");
        sb.setLength(sb.length()-1);
        return sb.toString();
    } // toString()

    
    /**
     * Create a prefix string for the textual representation.
     * 
     * @param level the level in the tree.
     * @return the prefix.
     */
    protected String prefix(int level) {
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < level; i++) sb.append("\t");
        return sb.toString();
    } // prefix()


    /**
     * Returns an iterable over the tuples returned by this operator.
     *
     * @return an iterable over the tuples of this operator.
     * @throws EngineException if the iterable cannot be constructed.
     */
    public Iterable<Tuple> tuples() throws EngineException {
        return new TupleIterable(this);
    }


    /**
     * A wrapper for iterable syntax over operators. Why? Because I
     * can.
     */
    class TupleIterable implements Iterable<Tuple> {
        /** Are there more elements in this iterator? */
        private boolean more;
        /** The next tuple to be returned. */        
        private Tuple tuple;

        private Operator operator;

        /**
         * Constructs a new iterable wrapper.
         *
         * @throws EngineException if the wrapper cannot be
         * constructed.
         */
        public TupleIterable(Operator op) throws EngineException {
            operator = op;
            tuple = operator.getNext();
            more = (tuple != null
                    ? (!(tuple instanceof EndOfStreamTuple))
                    : true);
        }

        /**
         * Returns the iterator implementation over the operator.
         *
         * @return the iterator over the tuples returned by this
         * operator.
         */
        public Iterator<Tuple> iterator() {
            return new Iterator<Tuple>(){
                /**
                 * Checks whether there are more tuples.
                 * @return <code>true</code> if there are more tuples,
                 * <code>false</code> otherwise.                 
                 */
                public boolean hasNext() {
                    return more;
                } // hasNext()
                /**
                 * Retrieves the next tuple from this iterator.
                 * @return the next tuple.
                 * @throws NoSuchElementException if there is
                 * something wrong when retrieving the tuple (most
                 * likely because we're requesting a tuple from an
                 * exhausted iterator).
                 */
                public Tuple next() throws NoSuchElementException {
                    try {
                        Tuple ret = tuple;
                        tuple = operator.getNext();
                        more = (tuple != null
                                ? (!(tuple instanceof EndOfStreamTuple))
                                : true);
                        return ret;
                    }
                    catch (EngineException ee) {
                        throw new NoSuchElementException("Cursor advance "
                                                         + "failed: "
                                                         + ee.getMessage());
                    }
                } // next()
                /**
                 * Removes the current tuple (unsupported -- will
                 * throw an exception by default).
                 * @throws UnsupportedOperationException by default --
                 * this is not supported by operators.
                 */
                public void remove() throws UnsupportedOperationException {
                    throw new UnsupportedOperationException("Cannot remove "
                                                            + "through "
                                                            + "an operator's "
                                                            + "iterator.");
                } // remove()
            }; // return new Iterator<Tuple>()
        } // iterator()
    } // TupleIterable

} // Operator
/*
 * Created on Dec 18, 2003 by sviglas
 *
 * Modified on Dec 24, 2008 by sviglas
 *
 * This is part of the attica project.  Any subsequent modification
 * of the file should retain this disclaimer.
 * 
 * University of Edinburgh, School of Informatics
 */





/**
 * PhysicalJoin: The base class for all joins.  Contains some
 * additional "plumbing" for join implementation.
 *
 * @author sviglas
 */
abstract class PhysicalJoin extends BinaryOperator {
    
    /** The storage manager for this operator. */
    private StorageManager sm;
	
    /** The predicate evaluated by this join operator. */
    private Predicate predicate;

	
    /**
     * Constructs a new physical join operator.
     * 
     * @param left the left input operator.
     * @param right the right input operator
     * @param sm the storage manager.
     * @param predicate the predicate evaluated by this join operator.
     * @throws EngineException thrown whenever the operator cannot be 
     * properly constructed.
     */
    public PhysicalJoin(Operator left, Operator right,
                        StorageManager sm, Predicate predicate) 
	throws EngineException {
        
        super(left, right);
        this.sm = sm;
        this.predicate = predicate;
    } // PhysicalJoin()


    /**
     * Retrieves the storage manager of this physical join.
     *
     * @return the storage manager.
     */
    protected StorageManager getStorageManager() {
        return sm;
    } // getStorageManager()


    /**
     * Retrieves the predicate of this physical join.
     *
     * @return the predicate.
     */
    protected Predicate getPredicate () {
        return predicate;
    } // getPredicate()

    
    /**
     * Sets the output relation for this operator.
     * 
     * @return the output relation for this operator.
     * @throws EngineException thrown whenever the output relation cannot
     * be constructed.
     */
    protected Relation setOutputRelation() throws EngineException {
        List<Attribute> attributes = new ArrayList<Attribute>();
        Relation rel = getInputOperator(LEFT).getOutputRelation();
        for (int i = 0; i < rel.getNumberOfAttributes(); i++)
            attributes.add(rel.getAttribute(i));
    
        rel = getInputOperator(RIGHT).getOutputRelation();
        for (int i = 0; i < rel.getNumberOfAttributes(); i++)
            attributes.add(rel.getAttribute(i));
		
        return new Relation(attributes);
    } // setOutputRelation()

} // PhysicalJoin
/*
 * Created on Dec 9, 2003 by sviglas
 *
 * Modified on Dec 24, 2008 by sviglas
 *
 * This is part of the attica project.  Any subsequent modification
 * of the file should retain this disclaimer.
 * 
 * University of Edinburgh, School of Informatics
 */




/**
 * Project: Implements standard attribute projection.
 *
 * @author sviglas
 */
class Project extends UnaryOperator {
	
    /** The slots of tuples to be projected. */	
    private int [] slots;

    /** Reusable return list. */
    private List<Tuple> returnList;
    
    /**
     * Constructs a new projection operator.
     * 
     * @param operator the input operator.
     * @param slots the slots to be projected.
     * @throws EngineException thrown whenever the projection operator
     * cannot be properly initialized.
     */
    public Project(Operator operator, int [] slots) throws EngineException {
        super(operator);
        this.slots = slots;
        returnList = new ArrayList<Tuple>();
    } // Project()

    /**
     * Processes an incoming tuple.
     * 
     * @param tuple the tuple to be processed.
     * @param inOp the index of the input operator the tuple to be
     * processed belongs to.
     * @throws EngineException on problems with projecting from the
     * input.
     */
    @Override
    protected List<Tuple> innerProcessTuple(Tuple tuple, int inOp) 
	throws EngineException {
        
        List<Comparable> newValues = new ArrayList<Comparable>();
        for (int i = 0; i < tuple.size(); i++) {
            if (containsSlot(i)) newValues.add(tuple.getValue(i));
        }

        // I have a bad feeling about this...
        returnList.clear();
        returnList.add(
            new Tuple(new IntermediateTupleIdentifier(tupleCounter++),
                      newValues));
        return returnList;
    } // innerProcessTuple()

    
    /**
     * Is the given slot contained in the projection array?
     * 
     * @param slot the slot to be checked.
     * @return <code>true</code> if the slot is contained in the projection
     * array, <code>false</code> otherwise.
     */
    protected boolean containsSlot(int slot) {
        
        boolean found = false;		
        for (int i = 0; i < slots.length && ! found; i++)
            found = (slots[i] == slot);
        return found;
    } // containsSlot()


    /**
     * Sets the output relation of this projection.
     *
     * @return the output relation of this projection.
     * @throws EngineException if the output relation cannot be
     * properly constructed.
     */
    @Override
    protected Relation setOutputRelation() throws EngineException {
        try {
            Operator incoming = getInputOperator();
            Relation inputRelation = incoming.getOutputRelation();
            List<Attribute> attrs = new ArrayList<Attribute>();
            for (int slot = 0;
                 slot < inputRelation.getNumberOfAttributes(); slot++) {
                if (containsSlot(slot)) {
                    //attrs.addElement(attr);
                    attrs.add(inputRelation.getAttribute(slot));
                    //System.out.println(attrs);
                }
            }
            return new Relation(attrs);
        }
        catch (Exception e) {
            throw new EngineException("Could not set the output relation.", e);
        }
    } // setOutputRelation()

    
    /**
     * Textual representation
     */
    @Override
    protected String toStringSingle() {
        StringBuffer sb = new StringBuffer();
        sb.append("project <");
        for (int i = 0; i < slots.length-1; i++)
            sb.append(slots[i] + ", ");
        if (slots.length >= 1) {
			sb.append(slots[slots.length-1]);
        }
        sb.append(">");
        return sb.toString();
    } // toStringSingle()
    
} // Project
/*
 * Created on Dec 13, 2003 by sviglas
 *
 * Modified on Dec 24, 2008 by sviglas
 *
 * This is part of the attica project.  Any subsequent modification
 * of the file should retain this disclaimer.
 * 
 * University of Edinburgh, School of Informatics
 */




/**
 * RelationScan: Scans an input relation from its primary file.
 *
 * @author sviglas
 */
class RelationScan extends SourceOperator {
    /** The storage manager for this scan. */
    private StorageManager sm;
    
    /** The relation this scanner scans. */
    private Relation relation;
	
    /** The filename of the file storing the relation. */
    private String filename;
	
    /** The relation manager for this scan. */
    private RelationIOManager inputMan;

    /** The iterator over the input file. */
    private Iterator<Tuple> tuples;

    /** Reusable return list. */
    private List<Tuple> returnList;
	
    /**
     * Constructs a new relation scan operator
     * 
     * @param sm this scanner's storage manager.
     * @param relation the relation this scanner scans.
     * @param filename the filename of the file that stores the
     * relation.
     * @throws EngineException thrown whenever the relation scanner
     * cannot be properly initialised.
     */
    public RelationScan(StorageManager sm, Relation relation, String filename) 
	throws EngineException {
        
        super();
        this.sm = sm;
        this.relation = relation;
        this.filename = filename;
        inputMan = new RelationIOManager(sm, relation, filename);
    } // RelationScan()

    
    /**
     * Fetch the filename that is to be scanned.
     * 
     * @return the filename this relation scanner scans.
     */
    public String getFileName() {
        return filename;
    } // getFileName()

    
    /**
     * Sets up the relation scan.
     * 
     * @throws EngineException whenever the relation scan cannot be
     * set up.
     */
    @Override
    protected void setup() throws EngineException {
        try {
            tuples = inputMan.tuples().iterator();
            returnList = new ArrayList<Tuple>();
        }
        catch (Exception sme) {
            throw new EngineException("Could not set up a relation scan.", sme);
        }
    } // setup()

    
    /**
     * Inner method to retrieve the next tuple(s).
     * 
     * @return an array of newly retrieved tuples.
     * @throws EngineException whenever the next tuple(s) cannot be
     * retrieved.
     */
    @Override
    protected List<Tuple> innerGetNext () throws EngineException {
        try {
            returnList.clear();
            if (tuples.hasNext()) returnList.add(tuples.next());
            else returnList.add(new EndOfStreamTuple());
            return returnList;
        }
        catch (Exception sme) {
            throw new EngineException("Could not fetch a tuples from "
                                      + "a relation scan.", sme);
        }
    } // innerGetNext()


    /**
     * Inner processing of a tuple (never called, but made idempotent
     * for safety because I'm kinda stupid.)
     *
     * @param tuple the tuple to be processed.
     * @param inOp the source of this tuple.
     * @return empty list by default.
     * @throws EngineException never thrown by default (legacy call)
     */
    @Override
    protected List<Tuple> innerProcessTuple(Tuple tuple, int inOp)
	throws EngineException {
        returnList.clear();
        return returnList;
    } // innerProcessTuple()
    

    /** 
     * Sets the output relation of this relation scan.
     * 
     * @return this relation scan's output relation.
     * @throws EngineException thrown whenever the output relation
     * cannot be set.
     */
    @Override
    protected Relation setOutputRelation() throws EngineException {
        return relation;
    } // setOutputRelation()

    
    /**
     * Textual representation.
     */
    protected String toStringSingle() {
        return "scan <" + filename + ">";
    } // toStringSingle()
	
    /**
     * Debug main
     * 
     * @param args arguments
     */
    public static void main (String [] args) {
        try {
            BufferManager bm = new BufferManager(100);
            StorageManager sm = new StorageManager(null, bm);
            
            List<Attribute> attributes = new ArrayList<Attribute>();
            attributes.add(new Attribute("integer", Integer.class));
            attributes.add(new Attribute("string", String.class));
            Relation relation = new Relation(attributes);
            String filename = args[0];
            
            RelationScan rs = new RelationScan(sm, relation, filename);
            boolean done = false;
            while (! done) {
                Tuple tuple = rs.getNext();
                System.out.println(tuple);
                done = (tuple instanceof EndOfStreamTuple);
            }
        }
        catch (Exception e) {
            System.err.println("Exception: " + e.getMessage());
            e.printStackTrace(System.err);
        }
    } // main()
} // RelationScan
/*
 * Created on Dec 10, 2003 by sviglas
 *
 * Modified on Dec 24, 2008 by sviglas
 *
 * This is part of the attica project.  Any subsequent modification
 * of the file should retain this disclaimer.
 * 
 * University of Edinburgh, School of Informatics
 */





/**
 * Select: Implementation of a selection operator.
 *
 * @author sviglas
 */
class Select extends UnaryOperator {
	
    /** The predicate this selection operator evaluates. */
    private Predicate predicate;

    /** Reusable return list. */
    private List<Tuple> returnList;
	
    /**
     * Constructs a new selection operator given its input.
     * 
     * @param operator the input operator.
     * @param predicate the predicate this selection operator
     * evaluates.
     * @throws EngineException thrown whenever the selection operator cannot
     * be properly constructed.
     */
    public Select (Operator operator, Predicate predicate) 
	throws EngineException {
        
        super(operator);
        this.predicate = predicate;
        returnList = new ArrayList<Tuple>();
    } // Select()

    
    /**
     * Processes an incoming tuple.
     * 
     * @param tuple the tuple to be processed. 
     * @param inOp the incoming operator this tuple belongs to (should
     * always default to '0').
     * @throws EngineException thrown whenever the tuple cannot be
     * evaluated
     */
    @Override
    protected List<Tuple> innerProcessTuple(Tuple tuple, int inOp)
	throws EngineException {
        
        // call the tuple inserter to insert the tuple into the predicate
        // for proper evaluation
        PredicateTupleInserter.insertTuple(tuple, predicate);
        boolean value = PredicateEvaluator.evaluate(predicate);
        returnList.clear();
        if (value) {
            Tuple out =
                new Tuple(new IntermediateTupleIdentifier(tupleCounter++),
                          tuple.getValues());
            returnList.add(out);
        }
        return returnList;
    } // innerProcessTuple()

    
    /**
     * Return a new relation for this operator's output relation.
     * 
     * @return the output relation of this operator.
     */
    @Override
    protected Relation setOutputRelation() throws EngineException {
        return new Relation(getInputOperator().getOutputRelation());
    } // SetOutputRelation()

    
    /**
     * Textual representation.
     */
    @Override
    protected String toStringSingle () {
        return "select <" + predicate + ">";
    } // toStringSingle()

} // Select
/*
 * Created on Dec 20, 2003 by sviglas
 *
 * Modified on Dec 24, 2008 by sviglas
 *
 * This is part of the attica project.  Any subsequent modification
 * of the file should retain this disclaimer.
 * 
 * University of Edinburgh, School of Informatics
 */





/**
 * Sink: An operator acting as a sink for other operators (i.e., it
 * simply saves its input and propagates it on getNext() calls).
 *
 * @author sviglas
 */
class Sink extends UnaryOperator {
    
    /** The storage manager for the sink. */
    private StorageManager sm;
	
    /** The temporary file to store the result. */
    private String filename;
    
    /** The manager that undertakes relation I/O. */
    private RelationIOManager man;

    /** The iterator over tuples of the output file. */
    private Iterator<Tuple> tuples;

    /** The reusable list of return tuples. */
    private List<Tuple> returnList;
    
    /**
     * Default constructor.
     *
     * @throws EngineException whenever the operator cannot be
     * constructed.
     */
    public Sink() throws EngineException {
        super();
    } // Sink()

    
    /**
     * Constructs a new sink operator.
     * 
     * @param operator the input operator to this sink.
     * @param sm this sink's storage manager.
     * @param filename the name of the file where the data will be
     * stored
     * @throws EngineException thrown whenever the operator cannot be
     * properly initialised.
     */
    public Sink(Operator operator, StorageManager sm, String filename) 
	throws EngineException {
        
        super(operator);
        this.sm = sm;
        this.filename = filename;
    } // Sink()

    
    /**
     * Sets up this sink operator.
     * 
     * @throws EngineException whenever the sink cannot be set up.
     */
    @Override
    protected void setup() throws EngineException {
        try {
            sm.createFile(filename);
            Relation rel = getInputOperator().getOutputRelation();
            man = new RelationIOManager(sm, rel, filename);
            boolean done = false;
            while (! done) {
                Tuple tuple = getInputOperator().getNext();
                if (tuple != null) {
                    done = (tuple instanceof EndOfStreamTuple);
                    if (! done) man.insertTuple(tuple);
                }
            }
            //man = new RelationIOManager(sm, rel, filename);
            // I should burn in hell for initialising fields after
            // construction, but I'll probably burn in hell anyway, so
            // what's one more reason...
            tuples = man.tuples().iterator();
            returnList = new ArrayList<Tuple>();
        }
        catch (IOException ioe) {
            throw new EngineException("Could not create output iterator.", ioe);
        }
        catch (StorageManagerException sme) {
            throw new EngineException("Could not store final output.", sme);
        } 
    } // setup()

    
    /**
     * Cleans up after all processing.
     * 
     * @throws EngineException thrown whenever the operator cannot
     * clean up.
     */
    @Override
    protected void cleanup() throws EngineException {
        try {
            sm.deleteFile(filename);
        }
        catch (StorageManagerException sme) {
            throw new EngineException("Could not clean up final output", sme);
        }
    } // cleanup()
    

    /**
     * The inner method to retrieve tuples
     * 
     * @return the newly retrieved tuples.
     * @throws EngineException thrown whenever the next iteration is not 
     * possible.
     */
    @Override
    protected List<Tuple> innerGetNext () throws EngineException {
        try {
            returnList.clear();
            if (tuples.hasNext()) returnList.add(tuples.next());
            else returnList.add(new EndOfStreamTuple());
            return returnList;
        }
        catch (Exception sme) {
            throw new EngineException("Could not read tuples "
                                      + "from intermediate file.", sme);
        }
    } // innerGetNext()

    
    /**
     * Operator abstract interface -- never called.
     */
    @Override
    protected List<Tuple> innerProcessTuple(Tuple tuple, int inOp)
	throws EngineException {
        returnList.clear();
        return returnList;
    } // innerProcessTuple()

    
    /**
     * Operator abstract interface -- sets the ouput relation of
     * this sink operator.
     * 
     * @return this operator's output relation.
     * @throws EngineException whenever the output relation of this
     * operator cannot be set.
     */
    @Override
    protected Relation setOutputRelation() throws EngineException {
        return new Relation(getInputOperator().getOutputRelation());
    } // setOutputRelation()

    
    /**
     * Textual representation
     */
    @Override
    public String toStringSingle() {
        return "sink <" + filename + ">";
    } // toStringSingle()
    
} // Sink
/*
 * Created on Dec 13, 2003 by sviglas
 *
 * Modified on Dec 24, 2008 by sviglas
 *
 * This is part of the attica project.  Any subsequent modification
 * of the file should retain this disclaimer.
 * 
 * University of Edinburgh, School of Informatics
 */


/**
 * SourceOperator: An operator that acts as a source to other
 * operators.
 *
 * @author sviglas
 */
abstract class SourceOperator extends Operator {
	
    /**
     * Constructs a new source operator.
     */
    public SourceOperator() throws EngineException {
        super();
    } // SourceOperator()

} // SourceOperator
/*
 * Created on Dec 9, 2003 by sviglas
 *
 * Modified on Dec 24, 2008 by sviglas
 *
 * This is part of the attica project.  Any subsequent modification
 * of the file should retain this disclaimer.
 * 
 * University of Edinburgh, School of Informatics
 */


/**
 * UnaryOperator: Models a unary engine operator.
 *
 * @author sviglas
 */
abstract class UnaryOperator extends Operator {

    /**
     * Constructs a new unary operator given an input operator.
     * 
     * @throws EngineException thrown whenever the unary operator
     * cannot be properly constructed.
     */
    public UnaryOperator() throws EngineException {
        
        super();
        List<Operator> inops = new ArrayList<Operator>();
        setInputs(inops);
    } // UnaryOperator()
    
    
    /**
     * Constructs a new unary operator given an input operator.
     * 
     * @param operator the input operator of this unary operator.
     * @throws EngineException thrown whenever the unary operator
     * cannot be properly constructed.
     */
    public UnaryOperator(Operator operator) throws EngineException {
        
        super();
        List<Operator> inops = new ArrayList<Operator>();
        inops.add(operator);
        setInputs(inops);
    } // UnaryOperator()


    /**
     * Returns the sole input operator of this unary operator.
     * 
     * @return this unary operator's sole input operator.
     * @throws EngineException ideally it should never be thrown.
     */
    public Operator getInputOperator() throws EngineException {
        return getInputOperator(0);
    } // getInputOperator()
	
} // UnaryOperator
/*
 * Created on Dec 14, 2003 by sviglas
 *
 * Modified on Dec 26, 2008 by sviglas
 *
 * This is part of the attica project.  Any subsequent modification
 * of the file should retain this disclaimer.
 * 
 * University of Edinburgh, School of Informatics
 */








/**
 * PlanBuilder: Given a collection of algebraic operators, constructs
 * an evaluation plan to be used by the engine.
 *
 * @author sviglas
 */
class PlanBuilder {
	
    /** The database catalog. */
    private Catalog catalog;
    
    /** The storage manager. */
    private StorageManager sm;
	
    /**
     * Constructs a new plan builder instance, given a catalog and
     * a storage manager.
     * 
     * @param catalog the database catalog.
     * @param sm the storage manager.
     */
    public PlanBuilder(Catalog catalog, StorageManager sm) {
        this.catalog = catalog;
        this.sm = sm;
    } // PlanBuilder()

    
    /**
     * Given a collection of algebraic operators, construct the
     * evaluation plan and return its root.
     * 
     * @param operators the collection of algebraic operators.
     * @return the root of the evaluation plan.
     * @throws PlanBuilderException thrown whenever a plan cannot be
     * constructed.
     */
    public Operator buildPlan(String filename,
                              List<AlgebraicOperator> operators) 
	throws PlanBuilderException {

        //
        // The process is quite simple: first decompose the operators
        // into scans, projections, selections, and joins (i.e., the
        // conjunctive normal form of the query -- which is rather
        // fortunate since this is all we support anyway). Once these
        // are identified, perform the bare minimum. Drop all
        // unneccessary fields so we can reduce tuple width, then
        // impose all selections, enumerate joins, and finally impose
        // any projection lists and sort orders. Join enumeration is
        // syntactic, i.e., the order we apply the joins is the order
        // these joins are specified in the query. There are a few
        // quirks in the optimisation process, but nothing special.
        // Most of it is simple bookkeeping.
        //
        
        Operator operator = null;
	
        // get the relevant table names
        Set<String> tables = getTables(operators);
        // get the projections
        List<Projection> projections = getProjections(operators);
        // get the selections
        List<Selection> selections = getSelections(operators);
        // get the joins
        List<Join> joins = getJoins(operators);
        List<Sort> sorts = getSorts(operators);
	
        // now, start building the plan
	
        // first build the initial projections
        List<InitialProjection> iProjections =
            buildInitialProjections(projections, selections, joins, tables);
        
        // given the tables, build the relation scans
        List<RelationScan> scans = buildScans(tables);
        // impose the initial projections over the relation scans
        List<Operator> ips =
            imposeInitialProjections(iProjections, scans);
        // now, append the relevant selections over each scan
        List<Operator> planOps = imposeSelections(selections, ips);
        // then, order the joins and cartesians in a deep tree
        planOps = enumerateJoins(joins, planOps);
        // perform sanity check and impose the final projections
        if (planOps.size() != 1) {
            throw new PlanBuilderException("Multiple branches after join "
                                           + "enumeration.");
        }
        operator = planOps.get(0);
        operator = imposeFinalProjections(projections, operator);
        operator = imposeSorts(sorts, operator);
	
        try {
            operator = new Sink(operator, sm, filename);
        }
        catch (EngineException ee) {
            throw new PlanBuilderException("Could not build final sink "
                                           + "operator (" + ee.getMessage()
                                           + ").", ee);
        }
	
        // we're done baby, we're done...
        return operator;
    } // buildPlan()
	
    /**
     * Given a physical operator, return a set with the names
     * of all tables participating in it.
     * 
     * @param operator the physical operator
     * @return a set of the names of all tables participating in the
     * physical operator.
     * @throws PlanBuilderException whenever the tables cannot be
     * fetched.
     */
    protected Set<String> getTables(Operator operator) 
        throws PlanBuilderException {
        
        try {
            Set<String> tables = new LinkedHashSet<String>();
		
            Relation rel = operator.getOutputRelation();
            for (Attribute attribute : rel) {
                TableAttribute tab = (TableAttribute) attribute;
                String table = tab.getTable();
                tables.add(table);
            }
            
            return tables;
        }
        catch (EngineException ee) {
            throw new PlanBuilderException("Could not obtain schema "
                                           + "information.", ee);
        }
    } // getTables()
	

    /**
     * Given a list of algebraic operators, return a set with the
     * names of all tables appearing in the operators.
     * 
     * @param operators a list of algebraic operators.
     * @return a set with the names of all tables appearing in the
     * algebraic operators.
     */
    protected Set<String> getTables(List<AlgebraicOperator> operators) {
        Set<String> inTables = new LinkedHashSet<String>();
        
        for (AlgebraicOperator alg : operators) {            
            if (alg instanceof Projection) {
                Projection p = (Projection) alg;
                for (Variable var : p.projections()) {
                    String table = var.getTable();
                    inTables.add(table);
                }
            }
            else if (alg instanceof Selection) {
                Selection s = (Selection) alg;
                VariableValueQualification vvq = 
                    (VariableValueQualification) s.getQualification();
                String table = vvq.getVariable().getTable();
                inTables.add(table);
            }
            else if (alg instanceof Join) {
                Join j = (Join) alg;
                VariableVariableQualification vvq =
                    (VariableVariableQualification) j.getQualification();
                String leftTable = vvq.getLeftVariable().getTable();
                String rightTable = vvq.getRightVariable().getTable();
                inTables.add(leftTable);
                inTables.add(rightTable);
            }
        }
	
        return inTables;
    } // getTables()

    
    /**
     * Given a list of projections, a list of selections and a list of
     * joins, return a vector of initial projections, i.e., the
     * projections that should be pushed down right above the scans.
     * 
     * @param projections a list of algebraic projections.
     * @param selections a list of algebraic selections.
     * @param joins a list of algebraic joins.
     * @param tables the set of tables names.
     * @return a list of new projection operators.
     */
    protected List<InitialProjection>
        buildInitialProjections(List<Projection> projections,
                                List<Selection> selections,
                                List<Join> joins,
                                Set<String> tables) {
        
        List<InitialProjection> initProjections =
            new ArrayList<InitialProjection>();
        
        for (String table : tables) {
            Set<Variable> plp = getAttributes(table, projections);
            Set<Variable> pls = getAttributes(table, selections);
            Set<Variable> plj = getAttributes(table, joins);
            plp.addAll(pls);
            plp.addAll(plj);
            // brain-damaged -- should be fixed
            InitialProjection ip =
                new InitialProjection(new ArrayList<Variable>(plp));
            initProjections.add(ip);
        }

        return initProjections;
    } // getInitialProjections()

    
    /**
     * Given a table name and a list of algebraic operators, 
     * return a projection list of all accessed attributes.
     * 
     * @param table the table name.
     * @param operators a list of algebraic operators.
     * @return a projection list of all accessed attributes.
     */
    protected Set<Variable>
        getAttributes(String table,
                      List<? extends AlgebraicOperator> operators) {

        Set<Variable> outList = new LinkedHashSet<Variable>();

        for (AlgebraicOperator alg : operators) {
            if (alg instanceof Projection) {
                Projection p = (Projection) alg;
                for (Variable var : p.projections()) {
                    String pTable = var.getTable();
                    if (table.equals(pTable)) outList.add(var);
                }
            }
            else if (alg instanceof Selection) {
                Selection s = (Selection) alg;
                VariableValueQualification vvq = 
                    (VariableValueQualification) s.getQualification();
                String sTable = vvq.getVariable().getTable();
                if (table.equals(sTable)) outList.add(vvq.getVariable());
            }
            else if (alg instanceof Join) {
                Join j = (Join) alg;
                VariableVariableQualification vvq =
                    (VariableVariableQualification) j.getQualification();
                String lTable = vvq.getLeftVariable().getTable();
                String rTable = vvq.getRightVariable().getTable();
                if (table.equals(lTable)) outList.add(vvq.getLeftVariable());
                if (table.equals(rTable)) outList.add(vvq.getRightVariable());
            }
        }
        
        return outList;
    } // getAttributes()

    
    /**
     * Given a set of tables, return a list of physical scan
     * operators.
     * 
     * @param tables a set of table names.
     * @return a list of table scan operators.
     * @throws PlanBuilderException thrown whenever the scans cannot
     * be constructed.
     */
    protected List<RelationScan> buildScans(Set<String> tables)
        throws PlanBuilderException {
        
        try {
            List<RelationScan> scans = new ArrayList<RelationScan>();

            for (String table : tables) {
                Table schema = catalog.getTable(table);
                String filename = catalog.getTableFileName(table);
                RelationScan rs = new RelationScan(sm, schema, filename);
                scans.add(rs);
            }
            
            return scans;
        }
        catch (NoSuchElementException nste) {
            throw new PlanBuilderException("Could not obtain schema "
                                           + "information.", nste);
        }
        catch (EngineException ee) {
            throw new PlanBuilderException("Could not instantiate relation "
                                           + "scans.", ee);
        }
    } // buildScans()
	
    /**
     * Imposes the initial projections over the scan operations.
     * 
     * @param iProjections the vector of initial projections.
     * @param scans the vector of scan operators.
     * @return a new vector of physical operators, with the
     * projections on top of the scans.
     * @throws PlanBuilderException thrown whenever the projections cannot
     * be imposed.
     */
    protected List<Operator>
        imposeInitialProjections(List<InitialProjection> iProjections,
                                 List<RelationScan> scans) 
	throws PlanBuilderException {
        
        try {
            List<Operator> branches = new ArrayList<Operator>();

            for (RelationScan rs : scans) {
                InitialProjection ip = 
                    getRelevantInitialProjection(iProjections, rs);
                Operator op = rs;
                if (ip != null) {
                    Project p =
                        new Project(rs, convertProjectionList(
                            ip.projections(), rs.getOutputRelation()));
                    op = p;
                }
                branches.add(op);
            }
            
            return branches;
        }
        catch (EngineException ee) {
            PlanBuilderException pbe =
                new PlanBuilderException("Could not instantiate initial projections");
            pbe.setStackTrace(ee.getStackTrace());
            throw pbe;
        }		
    } // imposeInitialProjections()

    
    /**
     * Given a list of projections and a scan, return the relevant 
     * projection.
     * 
     * @param ips the list of initial projections.
     * @param rs the relation scan.
     * @return the relevant initial projection.
     * @throws StorageManagerException thrown whenever the initial
     * projections cannot be identified.
     */
    protected InitialProjection
        getRelevantInitialProjection(List<InitialProjection> ips,
                                     RelationScan rs) 
	throws PlanBuilderException {
        
        try {
            for (InitialProjection ip : ips) {
                // get the table of the projection
                String pTable = ip.getProjectionList().get(0).getTable();
                // get the table of the scan
                TableAttribute tab = 
                    (TableAttribute) rs.getOutputRelation().getAttribute(0);
                String sTable = tab.getTable();
                if (pTable.equals(sTable)) return ip;
            }
            return null;
        }
        catch (EngineException ee) {
            throw new PlanBuilderException("Initial projections could not "
                                           + "be imposed.", ee);
        }
    } // getRelevantInitialProjection()

    
    /**
     * Imposes the selections over execution branches.
     * 
     * @param selections the list of selections.
     * @param sources the incoming plan sources.
     * @return a list of new execution branches with the selections
     * imposed.
     * @throws PlanBuilderException thrown whenever the selections
     * cannot be imposed.
     */
    protected List<Operator> imposeSelections(List<Selection> selections,
                                              List<? extends Operator> sources) 
	throws PlanBuilderException {
        
        try {
            List<Operator> res = new ArrayList<Operator>();
            
            for (Operator op : sources) {
                TableAttribute tab = 
                    (TableAttribute) op.getOutputRelation().getAttribute(0);
                String oTable = tab.getTable();
                List<Selection> rSelections =
                    getRelevantSelections(selections, oTable);
                if (rSelections.size() != 0) {
                    Operator top = op;
                    for (Selection sIt : rSelections) {
                        Select ps = new Select(top,
                            convertQualification(sIt.getQualification(), 
                                                 top));
                        res.add(ps);
                        top = ps;
                    }
                }
                else {
                    res.add(op);
                }
            }
            
            return res;
        }
        catch (EngineException ee) {
            throw new PlanBuilderException("Could not instantiate selections.",
                                           ee);
        }
    } // imposeSelections()

    
    /**
     * Given a list of selections and a table name, identify the
     * relevant selections on that table.
     * 
     * @param selections the list of selections.
     * @param table a table name.
     * @return a list containing all the selections over the given
     * table.
     */
    protected List<Selection>
        getRelevantSelections(List<Selection> selections, String table) {
        
        List<Selection> v = new ArrayList<Selection>();
	
        for (Selection s : selections) {
            VariableValueQualification vvq =
                (VariableValueQualification) s.getQualification();
            String sTable = vvq.getVariable().getTable();
            if (table.equals(sTable)) v.add(s);
        }
	
        return v;
    } // getRelevantSelections()

    
    /**
     * Given an iterable collection of variables a relation, it
     * converts the collection into an array of slots.
     * 
     * @param pl the projection list.
     * @param rel the relation schema.
     * @return an array of projection slots.
     * @throws PlanBuilderException thrown whenever the conversion cannot
     * be performed.
     */
    protected int [] convertProjectionList(Iterable<Variable> pl,
                                           Relation rel) 
	throws PlanBuilderException {
        
        try {
            List<Integer> slots = new ArrayList<Integer>();
            
            for (Variable v : pl) {
                int i = 0;
                for (Attribute a : rel) {
                    TableAttribute tab = (TableAttribute) a;
                    if (v.getTable().equals(tab.getTable())
                        && v.getAttribute().equals(tab.getName()))
                        slots.add(i);
                    i++;
                }
                /*
                  this was a bit braindead, but I'll leave it here for
                  posterity...
                  
                boolean done = false;
                for (int j = 0; j < rel.getNumberOfAttributes() && !done; j++) {
                    TableAttribute tab = (TableAttribute) rel.getAttribute(j);
                    if (v.getTable().equals(tab.getTable()) &&
                        v.getAttribute().equals(tab.getName())) {
                        slots.add(new Integer(j));
                        done = true;
                    }
                }
                */
            }

            int [] ret = new int[slots.size()];
            int i = 0;
            for (Integer in : slots) ret[i++] = in.intValue();
            return ret;
        }
        catch (ClassCastException cce) {
            throw new PlanBuilderException("Could not cast attributes.", cce);
        }
    } // convertProjectionList()

    
    /**
     * Converts a logical qualification to a physical predicate.
     * 
     * @param q the qualification to be converted.
     * @param op the incoming physical operator.
     * @return the physical predicate.
     * @throws PlanBuilderException thrown whenever the phyiscal
     * predicate cannot be constructed.
     */
    protected Predicate convertQualification(Qualification q, Operator op) 
	throws PlanBuilderException {
        
        try {
            Relation rel = op.getOutputRelation();
            if (q instanceof VariableValueQualification) {
                VariableValueQualification vvq =
                    (VariableValueQualification) q;
                Variable var = vvq.getVariable();
                TupleSlotPointer tsp = createSlotPointer(var, rel);
                Comparable c = createComparable(tsp.getType(), 
                                                vvq.getValue());
                return new TupleValueCondition(tsp, c, 
                    translateRelationship(vvq.getRelationship()));
            }
            else if (q instanceof VariableVariableQualification) {
                VariableVariableQualification vvq =
                    (VariableVariableQualification) q;
                Variable leftVar = vvq.getLeftVariable();
                TupleSlotPointer leftTsp = createSlotPointer(leftVar, rel);
                Variable rightVar = vvq.getRightVariable();
                TupleSlotPointer rightTsp = createSlotPointer(rightVar, rel);
                return new TupleTupleCondition(leftTsp, rightTsp,
                    translateRelationship(vvq.getRelationship()));
            }
            else {
                return new TrueCondition();
            }
        }
        catch (EngineException ee) {
            throw new PlanBuilderException("Could not obtain schema "
                                           + "information.", ee);
        }
    } // convertQualification()
    

    /**
     * Converts a qualification to a phyical predicate (used for joins).
     * 
     * @param q the qualification to be converted.
     * @param left the incoming left source.
     * @param right the incoming right source.
     * @return the resulting predicate.
     * @throws PlanBuilderException thrown whenever the predicate
     * cannot be constructed.
     */
    protected Predicate convertQualification(Qualification q, 
                                             Operator left,
                                             Operator right) 
	throws PlanBuilderException {
        
        try {
            Relation leftRel = left.getOutputRelation();
            Relation rightRel = right.getOutputRelation();
            if (q instanceof VariableVariableQualification) {
                VariableVariableQualification vvq = 
                    (VariableVariableQualification) q;
                Variable leftVar = vvq.getLeftVariable();
                TupleSlotPointer leftTsp = createSlotPointer(leftVar, leftRel);
                Variable rightVar = vvq.getRightVariable();
                TupleSlotPointer rightTsp = createSlotPointer(rightVar, rightRel);
                return new TupleTupleCondition(leftTsp, rightTsp,
                    translateRelationship(vvq.getRelationship()));
            }
            else {
                return new TrueCondition();
            }
        }
        catch (EngineException ee) {
            throw new PlanBuilderException("Could not obtain schema "
                                           + "information.", ee);
        }
    } // convertQualification()
    
	
    /**
     * Given a variable and a relation, it creates a slot pointer.
     * 
     * @param var the variable to be converted.
     * @param r the incoming relation.
     * @return the slot pointer pointing to the variable in the
     * relation.
     */
    protected TupleSlotPointer createSlotPointer(Variable var, Relation r) {
        int i = 0;
        String vTable = var.getTable();
        String vAttr = var.getAttribute();
        for (Attribute attr : r) {
            TableAttribute tab = (TableAttribute) attr;
            String rTable = tab.getTable();
            String rAttr = tab.getName();
            if (vTable.equals(rTable) && vAttr.equals(rAttr)) {
                return new TupleSlotPointer(tab.getType(), i);
            }
            i++;
        }
	
        return null;
    } // createSlotPointer()
    
	
    /**
     * Given a type and a value, it creates a comparable object for
     * use in the physical predicates.
     * 
     * @param type the type.
     * @param value the value of the logical qualification.
     * @return a comparable object to be used in a physical predicate.
     * @throws PlanBuilderException thrown whenever a comparable object
     * cannot be instantiated.
     */
    protected Comparable createComparable(Class<?> type, String value) 
	throws PlanBuilderException {
        
        try {
            if (type.equals(Byte.class)) return new Byte(value);
            else if (type.equals(Short.class)) return new Short(value);
            else if (type.equals(Character.class)) {
                if (value.length() != 1)
                    throw new PlanBuilderException("Could not build "
                                                   + "comparable value.");
                
                return new Character(value.charAt(0));
            }
            else if (type.equals(Integer.class)) return new Integer(value);
            else if (type.equals(Long.class)) return new Long(value);
            else if (type.equals(Float.class)) return new Float(value);
            else if (type.equals(Double.class)) return new Double(value);
            else if (type.equals(String.class)) return new String(value);
            else
                throw new PlanBuilderException("Could not build comparable "
                                               + "value.");
        }
        catch (Exception e) {
            throw new PlanBuilderException("Could not build comparable "
                                           + "value.", e);
        }
    } // createComparable()

    
    /**
     * Translator from logical to physical qualifications.
     * 
     * @param relationship the logical relationship between to values.
     * @return the physical relationship between the two values.
     */
    protected Condition.Qualification
        translateRelationship(Qualification.Relationship relationship) {
        
        switch (relationship) {
        case EQUALS:
            return Condition.Qualification.EQUALS;
        case NOT_EQUALS:
            return Condition.Qualification.NOT_EQUALS;
        case GREATER:
            return Condition.Qualification.GREATER;
        case GREATER_EQUALS:
            return Condition.Qualification.GREATER_EQUALS;
        case LESS:
            return Condition.Qualification.LESS;
        case LESS_EQUALS:
            return Condition.Qualification.LESS_EQUALS;
        }

        return Condition.Qualification.EQUALS;
    } // translateRelationship()
	
    protected Condition.Qualification
        reverseRelationship(Qualification.Relationship relationship) {
        
        switch (relationship) {
        case EQUALS:
            return Condition.Qualification.EQUALS;
        case NOT_EQUALS:
            return Condition.Qualification.NOT_EQUALS;
        case GREATER:
            return Condition.Qualification.LESS;
        case GREATER_EQUALS:
            return Condition.Qualification.LESS_EQUALS;
        case LESS:
            return Condition.Qualification.GREATER;
        case LESS_EQUALS:
            return Condition.Qualification.GREATER_EQUALS;
        }

        return Condition.Qualification.EQUALS;
    } // reverseRelationship()

    protected Qualification.Relationship
        reverseQR(Qualification.Relationship relationship) {
        
        switch (relationship) {
        case EQUALS:
            return Qualification.Relationship.EQUALS;
        case NOT_EQUALS:
            return Qualification.Relationship.NOT_EQUALS;
        case GREATER:
            return Qualification.Relationship.LESS;
        case GREATER_EQUALS:
            return Qualification.Relationship.LESS_EQUALS;
        case LESS:
            return Qualification.Relationship.GREATER;
        case LESS_EQUALS:
            return Qualification.Relationship.GREATER_EQUALS;
        }

        return Qualification.Relationship.EQUALS;
    } // reverseQR()

    
    /**
     * Enumerates the joins between a collection of sub-plans.
     * 
     * @param joins the logical joins to be enumerated.
     * @param branches the incoming sub-plans (branches).
     * @return a new vector of branches.
     * @throws PlanBuilderException thrown whenever join enumeration or
     * physical join construction is not possible.
     */
    protected List<Operator> enumerateJoins(List<Join> joins,
                                            List<Operator> branches)
	throws PlanBuilderException {
        
        try {
            if (branches.size() == 1 && joins.size() == 0) return branches;
            else if (branches.size() == 1 && joins.size() != 0)
                throw new PlanBuilderException("Could not enumerate joins");
            
            List<Operator> v = new ArrayList<Operator>();
			
            // pick a pair of branches and a predicate
            Triplet<Operator, Operator, List<Join>> triplet =
                pickPair(joins, branches);
            branches.remove(triplet.first);
            branches.remove(triplet.second);
            // build the join (?) operator
            if (triplet.third != null) {
                PhysicalJoin nlj = createJoin(triplet.first,
                                              triplet.second,
                                              triplet.third);
                List<Join> picked = triplet.third;
                for (Join it : picked) joins.remove(it);
                branches.add(nlj); 
            }
            else {
                // this is really a cartesian
                CartesianProduct cp = 
                    new CartesianProduct(triplet.first,
                                         triplet.second, sm);
                branches.add(cp);
            }
            
            return enumerateJoins(joins, branches);
        }
        catch (EngineException ee) {
            throw new PlanBuilderException("Could not enumerate joins.", ee);
        }
    } // enumerateJoins()
	
    /**
     * Given a collection of algebraic joins and a collection
     * of sub-plans, it picks a pair of branches to join.
     * 
     * @param joins a list of logical joins.
     * @param sources the incoming sub-plans.
     * @return a triplet consisting of the left and the right input
     * operators to the join, along with the algebraic join. The last
     * one is set to <code>null</code> if there are no more joins,
     * signifying that a Cartesian product should be built.
     * @throws PlanBuilderException thrown whenever a pair cannot be
     * picked.
     */
    protected Triplet<Operator, Operator, List<Join>>
        pickPair (List<Join> joins, List<Operator> sources) 
	throws PlanBuilderException {
        
        for (int i = 0; i < sources.size(); i++) {
            Operator left = sources.get(i);
            for (int j = i; j < sources.size(); j++) {
                Operator right = (Operator) sources.get(j);
                if (joined(left, right, joins))
                    return new Triplet<Operator, Operator, List<Join>>(
                        left, right, findJoins(left, right, joins));
            }
        }
		
        return new Triplet<Operator, Operator, List<Join>>(
            sources.get(0), sources.get(1), null);
    } // pickPair()
    
	
    /**
     * Given two operators (sub-plans) and a list of joins, it checks
     * if the two operators are joined.
     * 
     * @param left the left input.
     * @param right the right input.
     * @param joins a list of algebraic joins.
     * @return <code>true</code> if the sources are joined,
     * <code>false</code> otherwise.
     */
    protected boolean joined(Operator left, Operator right, List<Join> joins) 
	throws PlanBuilderException {
        
        for (Join join : joins) {
            // get the qualification
            VariableVariableQualification vvq =
                (VariableVariableQualification) join.getQualification();
            // get the two tables
            Variable leftVar = vvq.getLeftVariable();
            Variable rightVar = vvq.getRightVariable();
            String leftTable = leftVar.getTable();
            String rightTable = rightVar.getTable();
            // use getTables() to find the tables in each branch
            Set<String> leftTables = getTables(left);
            Set<String> rightTables = getTables(right);
            // check if these two tables are joined
            if ((leftTables.contains(leftTable) &&
                 rightTables.contains(rightTable))
                ||
                (leftTables.contains(rightTable) && 
                 rightTables.contains(leftTable)))
                return true;
        }
		
        return false;
    } // joined()

    
    /**
     * Given two operators and a collection of joins, return all joins
     * over the operators in a new vector.
     * 
     * @param left the left input operator.
     * @param right the right input operator.
     * @param joins all joins between the two input operators.
     * @return a vector with all the joins between the two input
     * operators.
     * @throws PlanBuilderException thrown whenever joins cannot be
     * identified.
     */
    protected List<Join> findJoins(Operator left, Operator right,
                                   List<Join> joins) 
	throws PlanBuilderException {
        
        List<Join> out = new ArrayList<Join>();
        Set<String> leftTables = getTables(left);
        Set<String> rightTables = getTables(right);
        
        for (Join join : joins) {
            // get the qualification
            VariableVariableQualification vvq =
                (VariableVariableQualification) join.getQualification();
            // get the two tables
            Variable leftVar = vvq.getLeftVariable();
            Variable rightVar = vvq.getRightVariable();
            String leftTable = leftVar.getTable();
            String rightTable = rightVar.getTable();
            if ((leftTables.contains(leftTable) && 
                 rightTables.contains(rightTable)) ||
                (leftTables.contains(rightTable) && 
                 rightTables.contains(leftTable))) {
                out.add(join);
            }
        }
		
        return out;
    } // findJoins()

    
    /**
     * Given two input operators and a collection of algebraic joins,
     * returns a physical join to evaluate them.
     * 
     * @param left the left input operator.
     * @param right the right input operator.
     * @param joins the joins between the two inputs.
     * @return a physical join to evaluate the join predicate.
     * @throws PlanBuilderException thrown whenever the physical join
     * cannot be instantiated.
     */
    protected PhysicalJoin createJoin(Operator left, 
                                      Operator right, 
                                      List<Join> joins) 
	throws PlanBuilderException {
        
        try {
            Predicate pred = null;
            boolean useSortMerge = false;
            
            if (joins.size() == 1) {
                Join join = joins.get(0);
                // single join, single predicate
                pred = createJoinPredicate(left, right, join);
                if (isMergeable(join)) useSortMerge = true;
            }
            else {
                // build a conjunction of all relevant predicates
                //
                // I have a feeling this will bite me in the ass when
                // dealing with multiple predicates over more than
                // three tables... (sviglas, 3/1/03)
                //
                // strange, it hasn't yet (sviglas, 4/1/08)
                //
                List<Predicate> preds = new ArrayList<Predicate>();
                for (Join join : joins)
                    preds.add(createJoinPredicate(left, right, join));
                pred = new Conjunction(preds);
            }

            if (! useSortMerge) {
                return new NestedLoopsJoin(left, right, sm, pred);
            }
            else {
                // sort-merge can be used, so build the plan for it

                // create the sort operations
                // figure out which goes where
                                
                // sanity check
                                
                if (joins.size() != 1) {
                    throw new PlanBuilderException("Trying to build a sort-"
                                                   + "merge over a "
                                                   + "non-equi-join");
                }
                
                Join join = joins.get(0);

                /////////////////////////////////////////////
                //
                //  uncomment the following and comment out
                //  nested loops when you're done
                //
                //////////////////////////////////////////////
                
                /*
                  Relation leftRel = left.getOutputRelation();
                  Relation rightRel = right.getOutputRelation();
                  VariableVariableQualification vvq =
                      (VariableVariableQualification) join.getQualification();
                  Variable leftVar = vvq.getLeftVariable();
                  Variable rightVar = vvq.getRightVariable();
                  TupleSlotPointer leftTsp = createSlotPointer(leftVar, leftRel);
                  TupleSlotPointer rightTsp = createSlotPointer(rightVar, rightRel);
                  int [] leftSlots = new int[1];
                  int [] rightSlots = new int[1];

                  if (leftTsp == null) {
                      // could not find the left join input in
                      // the left-hand side
                      // it should be in the right-hand side
                      leftTsp = createSlotPointer(leftVar, rightRel);
                      rightTsp = createSlotPointer(rightVar, leftRel);
                      // if it is still null, then something's wrong
                      if (leftTsp == null) {
                          throw new PlanBuilderException("Could not build "
                                                         + "merge-join");
                      }
                      else {
                          leftSlots[0] = rightTsp.getSlot();
                          rightSlots[0] = leftTsp.getSlot();
                      }
                  }
                  else {
                      leftSlots[0] = leftTsp.getSlot();
                      rightSlots[0] = rightTsp.getSlot();
                  }

                  /////////////////////////////////////////////////////
                  //
                  // Use your own implementation of sort here
                  //
                  /////////////////////////////////////////////////////
                  
                  int bufferPages = sm.getNumberOfBufferPoolPages();
                  int half = bufferPages / 2;
                  ExternalSort newLeft = new ExternalSort(left, sm, leftSlots,
                      half > 10 ? half : 10);
                  ExternalSort newRight = new ExternalSort(right, sm,
                      rightSlots, half > 10 ? half : 10);
                  // create the merge operation and combine it
                  pred = createJoinPredicate(newLeft, newRight, join);
                  return new MergeJoin(newLeft, newRight, sm, 
                                       leftSlots[0], rightSlots[0], pred);
                  */

                  pred = createJoinPredicate(left, right, join);
                  return new NestedLoopsJoin(left, right, sm, pred);
            }
        }
        catch (EngineException ee) {
            throw new PlanBuilderException("Could not instantiate physical "
                                           + "join", ee);
        }
    } // createJoin()


    /**
     * Checks whether sort-merge can be used or not.
     * 
     * @param join the logical join predicate to check
     * @return <code>true</code> if sort-merge can be used, 
     * <code>false</code> otherwise.
     */
    protected boolean isMergeable (Join join) {
        // basically, only equijoins for the time being
        // conjunctions could too be evaluated, but that
        // would make it all too complicated
        return join.getQualification().getRelationship() ==
            Qualification.Relationship.EQUALS;
    } // isMergeable()

    
    /**
     * Builds a single join predicate over two sources.
     * 
     * @param left the left input.
     * @param right the right input.
     * @param join the algebraic join.
     * @return the physical join predicate.
     * @throws PlanBuilderException thrown whenever the physical join
     * predicate cannot be instantiated.
     */
    protected Predicate createJoinPredicate(Operator left, 
                                            Operator right,
                                            Join join) 
	throws PlanBuilderException {
        
        try {
            VariableVariableQualification vvq =
                (VariableVariableQualification) join.getQualification();
            Variable leftVar = vvq.getLeftVariable();
            Variable rightVar = vvq.getRightVariable();
            Relation leftRel = left.getOutputRelation();
            Relation rightRel = right.getOutputRelation();
            TupleSlotPointer leftTsp = createSlotPointer(leftVar, leftRel);
            TupleSlotPointer rightTsp = createSlotPointer(rightVar, rightRel);
            Condition.Qualification qual =
                translateRelationship(vvq.getRelationship());
            // failed instantiation, we need to reverse the predicate
            if (leftTsp == null && rightTsp == null) {
                // this recursive call will succeed
                return createJoinPredicate(left, right,
                    new Join(
                        new VariableVariableQualification(reverseQR(
                            vvq.getRelationship()), rightVar, leftVar)));
            }
            return new TupleTupleCondition(leftTsp, rightTsp, qual);
        }
        catch (EngineException ee) {
            throw new PlanBuilderException("Could not obtain schema "
                                           + "information.", ee);
        }
    } // createJoinPredicate()

    
    /**
     * Given a list of algebraic operators, identify the projections
     * and return them in a new list.
     * 
     * @param operators the list of algebraic operators.
     * @return the projections in a new list.
     */
    protected List<Projection>
        getProjections(List<AlgebraicOperator> operators) {
        
        List<Projection> v = new ArrayList<Projection>();
        for (AlgebraicOperator alg : operators) 
            if (alg instanceof Projection) v.add((Projection) alg);
        
        return v;
    } // getProjections()

    
    /**
     * Given a list of algebraic operators, identify the selections
     * and return them in a new list.
     * 
     * @param operators the list of algebraic operators
     * @return the selections in a new list.
     */
    protected List<Selection> getSelections(List<AlgebraicOperator> operators) {

        List<Selection> v = new ArrayList<Selection>();
        for (AlgebraicOperator alg : operators)
            if (alg instanceof Selection) v.add((Selection) alg);

        return v;
    } // getSelections()

    
    /**
     * Given a list of algebraic operators, identify the joins
     * and return them in a new list.
     * 
     * @param operators the list of algebraic operators.
     * @return the joins in a new list.
     */
    protected List<Join> getJoins(List<AlgebraicOperator> operators) {
        
        List<Join> v = new ArrayList<Join>();
        for (AlgebraicOperator alg : operators)
            if (alg instanceof Join) v.add((Join) alg);

        return v;
    } // getJoins()

    /**
     * Given a list of algebraic operators, identify the sorts
     * and return them in a new list.
     * 
     * @param operators the list of algebraic operators.
     * @return the sorts in a new list.
     */
    protected List<Sort> getSorts(List<AlgebraicOperator> operators) {

        List<Sort> v = new ArrayList<Sort>();
        for (AlgebraicOperator alg : operators)
            if (alg instanceof Sort) v.add((Sort) alg);

        return v;
    } // getSorts()

    
    /**
     * Imposes the final result projection.
     * 
     * @param projections the list of final projections.
     * @param operator the incoming operator.
     * @return the new operator with the projection imposed.
     * @throws PlanBuilderException thrown whenever the projection
     * cannot be instantiated.
     */
    protected Operator imposeFinalProjections(List<Projection> projections,
                                              Operator operator) 
	throws PlanBuilderException {
        try {
            List<Variable> pl = createProjectionList(projections);
            Relation relation = operator.getOutputRelation();
            int [] slots = convertProjectionList(pl, relation);
            Project p = new Project(operator, slots);
            return p;
        }
        catch (EngineException ee) {
            throw new PlanBuilderException("Could not instantiate final " +
                                           "projection.", ee);
        }
    } // imposeFinalProjections()


    /**
     * Imposes the final sort.
     *
     * @param sorts the list of sort operations.
     * @param operator the incoming top operator.
     * @throws PlanBuilderException thrown whenever the final sort
     * cannot be instantiated.
     */
    protected Operator imposeSorts(List<Sort> sorts, Operator operator)
        throws PlanBuilderException {
        try {
            if (sorts.size() > 1) {
                throw new PlanBuilderException("More than one sort clauses.");
            }
            else if (sorts.size() == 1) {                
                Sort sort = sorts.get(0);
                List<Variable> sl = sort.getSortList();
                Relation relation = operator.getOutputRelation();
                int [] slots = convertProjectionList(sl, relation);

                ///////////////////////////////////////////
                //
                // you should substitute the null
                // operator for external sort here
                //
                ///////////////////////////////////////////
                
                ///////////////////////////////////////////
                //
                // this is an example with 10 buffers
                //
                ///////////////////////////////////////////
                
                int bufferPages = sm.getNumberOfBufferPoolPages();
                int half = bufferPages / 2;
                ExternalSort es =
                    new ExternalSort(operator, sm, slots,
                                     half > 10 ? half : 10);
                System.out.println("sssssssss");
                return es;
                
                ///////////////////////////////////////////
                /*
NullUnaryOperator nullop = new NullUnaryOperator(operator);
                return nullop;
*/
            }
            else {
                // just in case the sky falls
                return operator;
            }
                
        }
        catch (EngineException ee) {
            throw new PlanBuilderException("Cound not instantiate final "
                                           + "sort.", ee);
        }
    }
	

    /**
     * Given a list of projections, it collapses their projection
     * lists into a single one.
     * 
     * @param projections the list of algebraic projections.
     * @return a single projection list of all the projections lists
     * combined.
     */
    protected List<Variable>
        createProjectionList(List<Projection> projections) {

        // yeah yeah, I know, this isn't 1.5 generified and all that
        // crap...
        List<Variable> pl = projections.get(0).getProjectionList();
        for (int i = 1; i < projections.size(); i++) {
            Projection p = projections.get(i);
            pl.addAll(p.getProjectionList());
        }
	
        return pl;
    } // createProjectionList()
    
    /**
     * Debug main
     * 
     * @param args arguments
     */
    public static void main (String [] args) {
        try {
            org.dejave.attica.server.Database.ATTICA_DIR =
                System.getProperty("user.dir");
            String prefix = args[0] + System.getProperty("path.separator");
            // build the catalog
            Catalog catalog = new Catalog(prefix + "attica.cat");
            // build the buffer manager
            org.dejave.attica.storage.BufferManager bm = 
                new org.dejave.attica.storage.BufferManager(20);
            // build the storage manager
            StorageManager sm = new StorageManager(catalog, bm);
            // construct a database
            List<Attribute> attributes1 = new ArrayList<Attribute>();
            attributes1.add(new TableAttribute("table1", "key", Integer.class));
            attributes1.add(new TableAttribute("table1", "value",
                                               String.class));
            List<Attribute> attributes2 = new ArrayList<Attribute>();
            attributes2.add(new TableAttribute("table2", "key", Integer.class));
            attributes2.add(new TableAttribute("table2", "value",
                                               String.class));
            List<Attribute> attributes3 = new ArrayList<Attribute>();
            attributes3.add(new TableAttribute("table3", "key", Integer.class));
            attributes3.add(new TableAttribute("table3", "value",
                                               String.class));
            Table table1 = new Table("table1", attributes1);
            System.out.println(table1.getAttribute(0));
            Table table2 = new Table("table2", attributes2);
            Table table3 = new Table("table3", attributes3);
            org.dejave.attica.storage.CatalogEntry entry1 =
                new org.dejave.attica.storage.CatalogEntry(table1);
            org.dejave.attica.storage.CatalogEntry entry2 =
                new org.dejave.attica.storage.CatalogEntry(table2);
            org.dejave.attica.storage.CatalogEntry entry3 =
                new org.dejave.attica.storage.CatalogEntry(table3);
            catalog.createNewEntry(entry1);
            catalog.createNewEntry(entry2);
            catalog.createNewEntry(entry3);
            String filename1 = entry1.getFileName();
            sm.createFile(filename1);
            System.out.println(filename1 + " successfully created");
            String filename2 = entry2.getFileName();
            sm.createFile(filename2);
            System.out.println(filename2 + " successfully created");
            String filename3 = entry3.getFileName();
            sm.createFile(filename3);
            System.out.println(filename3 + " successfully created");
            org.dejave.attica.storage.RelationIOManager man1 = 
                new org.dejave.attica.storage.RelationIOManager(sm,
                                                                table1,
                                                                filename1);
            org.dejave.attica.storage.RelationIOManager man2 = 
                new org.dejave.attica.storage.RelationIOManager(sm,
                                                                table2,
                                                                filename2);
            org.dejave.attica.storage.RelationIOManager man3 = 
                new org.dejave.attica.storage.RelationIOManager(sm,
                                                                table3,
                                                                filename3);
            // populate it
            for (int i = 0; i < 100; i++) {
                List<Comparable> v1 = new ArrayList<Comparable>();
                v1.add(new Integer(i));
                v1.add(new String((v1.get(0)).toString()));
                org.dejave.attica.storage.Tuple tuple1 = 
                    new org.dejave.attica.storage.Tuple(
                        new org.dejave.attica.storage.TupleIdentifier(filename1,
                                                                      i),
                        v1);
                List<Comparable> v2 = new ArrayList<Comparable>();
                v2.add(new Integer(i % 5));
                v2.add(new String((v2.get(0)).toString()));
                org.dejave.attica.storage.Tuple tuple2 = 
                    new org.dejave.attica.storage.Tuple(
                        new org.dejave.attica.storage.TupleIdentifier(filename2,
                                                                      i),
                        v2);				
                List<Comparable> v3 = new ArrayList<Comparable>();
                v3.add(new Integer(i % 20));
                v3.add(new String((v3.get(0)).toString()));
                org.dejave.attica.storage.Tuple tuple3 = 
                    new org.dejave.attica.storage.Tuple(
                        new org.dejave.attica.storage.TupleIdentifier(filename3,
                                                                      i),
                        v3);
                man1.insertTuple(tuple1);
                //System.out.println("inserted " + tuple1);
                man2.insertTuple(tuple2);
                //System.out.println("inserted " + tuple2);
                man3.insertTuple(tuple3);
                //System.out.println("inserted " + tuple3);
            }
            // build an algebraic evaluation plan
            List<AlgebraicOperator> algebra =
                new ArrayList<AlgebraicOperator>();
            Join join1 =
                new Join(new VariableVariableQualification(
                    Qualification.Relationship.EQUALS,
                    new Variable("table1", "key"),
                    new Variable("table2", "key")));
            Join join2 = new Join(new VariableVariableQualification(
                    Qualification.Relationship.EQUALS,
                    new Variable("table2", "key"),
                    new Variable("table3", "key")));
            Selection sel = new Selection(new VariableValueQualification(
                    Qualification.Relationship.EQUALS,
                    new Variable("table3", "value"),
                    new String("0")));
            //Variable [] var = new Variable[2];
            List<Variable> pl = new ArrayList<Variable>();
            //var[0] = new Variable("table2", "value");
            //var[1] = new Variable("table1", "value");
            pl.add(new Variable("table3", "value"));
            Projection p = new Projection(pl);
            //algebra.addElement(sel);
            algebra.add(p);
            //algebra.addElement(join1); 
            //algebra.addElement(join2);
            System.out.println(algebra);
            // convert it to a physical plan
            PlanBuilder pb = new PlanBuilder(catalog, sm);
            Operator operator = pb.buildPlan("lala", algebra);
            System.out.println(operator);
            // evaluate it
            System.out.println("evaluating...");
            System.out.println("results:");
            boolean done = false;
            while (! done) {
                org.dejave.attica.storage.Tuple tuple = operator.getNext();
                done = (tuple instanceof EndOfStreamTuple);
            }
        }
        catch (Exception e) {
            System.err.println("Exception: " + e.getMessage());
            e.printStackTrace(System.err);
        }
    } // main()

} // PlanBuilder
/*
 * Created on Dec 14, 2003 by sviglas
 *
 * Modified on Dec 26, 2008 by sviglas
 *
 * This is part of the attica project.  Any subsequent modification
 * of the file should retain this disclaimer.
 * 
 * University of Edinburgh, School of Informatics
 */

/**
 * PlanBuilderException: Exception thrown when building a plan.
 *
 * @author sviglas
 */
class PlanBuilderException extends Exception {
	
    /**
     * Constructs a new exception instance.
     * 
     * @param msg this exception's message.
     */
    public PlanBuilderException(String msg) {
        super(msg);
    } // PlanBuilderException()

    
    /**
     * Constructs a new exception instance given a message and an
     * originating throwable.
     *
     * @param msg the message.
     * @param e the originating throwable.
     */
    public PlanBuilderException(String msg, Throwable e) {
        super(msg, e);
    } // PlanBuilderException()

} // PlanBuilderException
/*
 * Created on Dec 9, 2003 by sviglas
 *
 * Modified on Dec 26, 2008 by sviglas
 *
 * This is part of the attica project.  Any subsequent modification
 * of the file should retain this disclaimer.
 * 
 * University of Edinburgh, School of Informatics
 */

/**
 * Condition: A basic condition between two values.
 *
 * @author sviglas
 */
class Condition implements Predicate {

    public enum Qualification {
        EQUALS, NOT_EQUALS, GREATER, LESS, GREATER_EQUALS, LESS_EQUALS
    }
	
    /** A comparable left-hand side value. */
    private Comparable left;
	
    /** A comparable right-hand side value. */
    private Comparable right;
	
    /** The qualification between the two values. */
    private Qualification qualification;
    
    /**
     * Constructs a new condition without any arguments
     */
    public Condition() {
        this(null, null, Qualification.EQUALS);
    } // Condition() 
	

    /**
     * Constructs a new condition with two Java
     * <code>Comparable</code> objects as arguments.
     * 
     * @param left the left-hand side <code>Comparable</code> object.
     * @param right the right-hand side <code>Comparable</code>
     * object.
     * @param qualification the qualification between the two
     * <code>Comparable</code> objects.
     */
    public Condition(Comparable left, Comparable right,
                     Qualification qualification) {
        this.left = left;
        this.right = right;
        this.qualification = qualification;
    } // Condition()
	
    /**
     * Implement the <code>Predicate</code> interface by defining
     * the </code>evaluate()</code> method.
     * 
     * @return <code>true</code> if the predicate evaluates to true,
     * <code>false</code> otherwise.
     */
    @SuppressWarnings("unchecked")
    public boolean evaluate() {
        int value = 0;
        try {
            value = left.compareTo(right);
        }
        catch (ClassCastException cce) {
            return false;
        }

        switch (qualification) {
        case EQUALS: 
            return (value == 0);
        case NOT_EQUALS: 
            return (value != 0);
        case GREATER:
            return (value > 0);
        case LESS:
            return (value < 0);
        case GREATER_EQUALS:
            return (value >= 0);
        case LESS_EQUALS:
            return (value <= 0);
        }
        
        return false;
    } // evaluate()
	
    /**
     * Textual representation.
     * 
     * @return this condition's textual representation
     */
    @Override
    public String toString () {
        return "" + left + symbolString() + right;
    } // toString()

    
    /**
     * Convert the qualification to a string.
     * 
     * @return the symbol of the qualification as a string.
     */
    protected String symbolString () {
        switch (qualification) {
        case EQUALS: 
            return "=";
        case NOT_EQUALS: 
            return "!=";
        case GREATER:
            return ">";
        case LESS:
            return "<";
        case GREATER_EQUALS:
            return ">=";
        case LESS_EQUALS:
            return "<=";
        }
	
        return "?";
    } // symbolString()
} // Condition
/*
 * Created on Dec 10, 2003 by sviglas
 *
 * This is part of the attica project.  Any subsequent modification
 * of the file should retain this disclaimer.
 * 
 * University of Edinburgh, School of Informatics
 */


/**
 * Conjunction: A conjunction of predicates.
 *
 * @author sviglas
 */
class Conjunction extends ListPredicate implements Predicate {
	
    /**
     * Constructs a new conjunction.
     * 
     * @param list the list of predicates to be conjuncted.
     */
    public Conjunction(List<Predicate> list) {
        super(list);
    } // Conjunction()
	
    /**
     * Implements the <code>Predicate</code> interface by defining
     * the </code>evaluate()</code> method.
     * 
     * @return <code>true</code> if the predicate evaluates to true,
     * <code>false</code> otherwise.
     */
    public boolean evaluate() {
        
        for (Predicate predicate : predicates())
            if (! predicate.evaluate()) return false;
        return true;
    } // evaluate()

    
    /**
     * The symbol of the list.
     * 
     * @return this list's symbol.
     */
    @Override
    public String listSymbol() {
        return "AND";
    } // listSymbol()

} // Conjunction
/*
 * Created on Dec 10, 2003 by sviglas
 *
 * Modified on Dec 26, 2008 by sviglas
 * 
 * This is part of the attica project.  Any subsequent modification
 * of the file should retain this disclaimer.
 * 
 * University of Edinburgh, School of Informatics
 */


/**
 * Disjunction: A disjunction of predicates.
 *
 * @author sviglas
 */
class Disjunction extends ListPredicate implements Predicate {
    
    /**
     * Constructs a new disjunction of predicates.
     * 
     * @param list the list of predicates to be disjuncted.
     */
    public Disjunction(List<Predicate> list) {
        super(list);
    } // Disjunction()

    
    /**
     * Implements the <code>Predicate</code> interface by defining
     * the </code>evaluate()</code> method.
     * 
     * @return <code>true</code> if the predicate evaluates to true,
     * <code>false</code> otherwise.
     */
    public boolean evaluate() {

        for (Predicate predicate : predicates())
            if (predicate.evaluate()) return true;
        return false;
    } // evaluate()

    
    /**
     * The symbol of the list.
     * 
     * @return this list's symbol.
     */
    @Override
    public String listSymbol() {
        return "OR";
    } // listSymbol()

} // Disjunction
/*
 * Created on Dec 11, 2003 by sviglas
 *
 * Modified on Dec 26, 2008 by sviglas
 *
 * This is part of the attica project.  Any subsequent modification
 * of the file should retain this disclaimer.
 * 
 * University of Edinburgh, School of Informatics
 */


/**
 * ListPredicate: A predicate over a list of predicates.
 *
 * @author sviglas
 */
abstract class ListPredicate {
	
    /** The predicate list. */
    private List<Predicate> list;
	
    /**
     * Constructs a new list predicate.
     * 
     * @param list the predicate list.
     */
    public ListPredicate(List<Predicate> list) {
        this.list = list;
    } // ListPredicate()

    
    /**
     * Returns the length of the predicate list.
     * 
     * @return the predicate list's length
     */
    public int getPredicateListLength() {
        return list.size();
    } // getPredicateListLength()

    
    /**
     * Returns the requested predicate.
     * 
     * @param i the index of the predicate to be retrieved.
     * @return the index-th predicate.
     */
    public Predicate getPredicate(int i) {
        return list.get(i);
    } // getPredicate()

    
    /**
     * Sets the given predicate of the predicate list.
     * 
     * @param i the index of the predicate to be set.
     * @param predicate the new predicate.
     */
    public void setPredicate(int i, Predicate predicate) {
        list.set(i, predicate);
    } // setPredicate()


    /**
     * Returns the iterable list of predicates.
     *
     * @return the iterable list of predicate.
     */
    public Iterable<Predicate> predicates() {
        return list;
    } // getList()
    
    
    /**
     * Returns the symbol of this list.
     * 
     * @return this list's symbol.
     */
    protected String listSymbol() {
        return "??";
    } // listSymbol()

    
    /**
     * Textual representation.
     * 
     * @return this conjunction's textual representation.
     */
    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append("((" + getPredicate(0) +")");
        for (int i = 1; i < getPredicateListLength(); i++) {
            sb.append(listSymbol() + "(" + getPredicate(i) +")");
        }
        sb.append(")");
        return sb.toString();
    } // toString()

} // ListPredicate
/*
 * Created on Dec 10, 2003 by sviglas
 *
 * Modified on Dec 26, 2008 by sviglas
 * 
 * This is part of the attica project.  Any subsequent modification
 * of the file should retain this disclaimer.
 * 
 * University of Edinburgh, School of Informatics
 */

/**
 * Negation: The negation of a Predicate.
 *
 * @author sviglas
 */
class Negation implements Predicate {
	
    /** The predicate to be negated. */
    private Predicate predicate;
    
    /**
     * Constructs a new negation.
     * 
     * @param predicate the predicate to be negated.
     */
    public Negation(Predicate predicate) {
        this.predicate = predicate;
    } // Negation()
	

    /**
     * Returns the predicate to be negated.
     * 
     * @return the negated predicate.
     */
    public Predicate getPredicate() {
        return predicate;
    } // getPredicate()

    
    /**
     * Sets the predicate to be negated.
     * 
     * @param predicate the new predicate to be negated.
     */
    public void setPredicate(Predicate predicate) {
        this.predicate = predicate;
    } // setPredicate()

    
    /**
     * Implements the <code>Predicate</code> interface by defining the
     * </code>evaluate()</code> method.
     * 
     * @return <code>true</code> if the negation evaluates to true,
     * <code>false</code> otherwise.
     */
    public boolean evaluate() {
        return ! predicate.evaluate();
    } // Negation()


    /**
     * Textual representation.
     */
    @Override
    public String toString() {
        return "NOT (" + predicate + ")";
    } // toString()

} // Negatiion
/*
 * Created on Dec 10, 2003 by sviglas
 *
 * Modified on Dec 26, 2008 by sviglas
 *
 * This is part of the attica project.  Any subsequent modification
 * of the file should retain this disclaimer.
 * 
 * University of Edinburgh, School of Informatics
 */

/**
 * Predicate: Access to the evaluate() method -- all predicate classes
 * should implement this interface.
 *
 * @author sviglas
 */
interface Predicate {
	
    /**
     * Method called to return true or false depending on the
     * implementation of the interface.
     * 
     * @return <code>true</code> if the predicate evaluates to 
     * <code>true</code>, <code>false</code> otherwise.
     */
    public boolean evaluate();

} // Predicate
/*
 * Created on Dec 9, 2003 by sviglas
 *
 * Modified on Dec 26, 2008 by sviglas
 *
 * This is part of the attica project.  Any subsequent modification
 * of the file should retain this disclaimer.
 * 
 * University of Edinburgh, School of Informatics
 */

/**
 * PredicateEvaluator: The predicate evaluator for the entire system.
 *
 * @author sviglas
 */
class PredicateEvaluator {
	
    /**
     * Convenience method to evaluate predicates.
     * 
     * @param pred the predicate to be evaluated.
     * @return <code>true</code> if the predicate holds,
     * <code>false</code> if it does not.
     */
    public static boolean evaluate(Predicate pred) {
        return pred.evaluate();
    } // evaluate()

} // PredicateEvaluator
/*
 * Created on Dec 11, 2003 by sviglas
 *
 * Modified on Dec 26, 2008 by sviglas
 *
 * This is part of the attica project.  Any subsequent modification
 * of the file should retain this disclaimer.
 * 
 * University of Edinburgh, School of Informatics
 */


/**
 * PredicateTupleInserter: It inserts one or more tuples into a
 * predicate depending on whether the predicate is a tuple/value or a
 * tuple/tuple predicate.
 *
 * @author sviglas
 */
class PredicateTupleInserter {
	
    /**
     * Inserts only a single tuple in the given predicate.
     * 
     * @param leftTuple the tuple to be inserted.
     * @param predicate the predicate to be modified.
     */
    public static void insertTuple(Tuple leftTuple,
                                   Predicate predicate) {
        insertTuples(leftTuple, null, predicate);
    } // insertTuple
    
    
    /**
     * Inserts two tuples in the given predicate.
     * 
     * @param leftTuple the left tuple of the predicate.
     * @param rightTuple the right tuple of the predicate.
     * @param predicate the predicate to be modified.
     */
    public static void insertTuples(Tuple leftTuple,
                                    Tuple rightTuple,
                                    Predicate predicate) {
        if (predicate instanceof TupleValueCondition) {
            // this is a tuple/value predicate -- set only the left tuple
            TupleValueCondition tvc = (TupleValueCondition) predicate;
            tvc.setTuple(leftTuple);
        }
        else if (predicate instanceof TupleTupleCondition) {
            // this is a tuple/tuple predicate -- set both tuples
            TupleTupleCondition ttc = (TupleTupleCondition) predicate;
            ttc.setTuples(leftTuple, rightTuple);
        }
        else if (predicate instanceof ListPredicate) {
            // this is a list of predicates -- recurse into the list
            // setting the tuples
            ListPredicate lp = (ListPredicate) predicate;
            for (Predicate pred : lp.predicates())
                insertTuples(leftTuple, rightTuple, pred);
        }
        else if (predicate instanceof Negation) {
            // this is a negation -- modify the negated predicate
            Negation n = (Negation) predicate;
            Predicate p = n.getPredicate();
            insertTuples(leftTuple, rightTuple, p);
            n.setPredicate(p);
        }
    } // insertTuples()
    
} // PredicateTupleInserter
/*
 * Created on Dec 14, 2003 by sviglas
 *
 * This is part of the attica project.  Any subsequent modification
 * of the file should retain this disclaimer.
 * 
 * University of Edinburgh, School of Informatics
 */

/**
 * TrueCondition: A condition that always evaluates to true.
 *
 * @author sviglas
 */
class TrueCondition implements Predicate {
    
    /**
     * Implementation of the <code>Predicate</code> interface.
     * 
     * @return <code>true</code> by default.
     */
    public boolean evaluate() {
        return true;
    } // evaluate()

    
    /**
     * Textual representation.
     * 
     * @return textual representation.
     */
    @Override
    public String toString() {
        return "TRUE";
    } // toString()
} // TrueCondition
/*
 * Created on Dec 10, 2003 by sviglas
 *
 * Modified on Dec 26, 2008 by sviglas
 *
 * This is part of the attica project.  Any subsequent modification
 * of the file should retain this disclaimer.
 * 
 * University of Edinburgh, School of Informatics
 */

/**
 * TupleSlotPointer: A pointer to a slot of a tuple.
 *
 * @author sviglas
 */
class TupleSlotPointer {

    /** The slot type. */
    private Class<? extends Comparable> type;
	
    /** The slot number. */
    private int slot;

    
    /**
     * Constructs a new tuple slot pointer given the slot.
     *
     * @param type the type of the slot.
     * @param slot the number of the slot.
     */
    public TupleSlotPointer(Class<? extends Comparable> type, int slot) {
        this.type = type;
        this.slot = slot;
    } // TupleSlotPointer()

    
    /**
     * Returns the slot this pointer points to.
     * 
     * @return this pointer's slot.
     */
    public int getSlot() {
        return slot;
    } // getSlot()


    /**
     * Returns the type of this slot.
     *
     * @return this slot's type.
     */
    public Class<? extends Comparable> getType() {
        return type;
    } // getType()
    
	
    /**
     * Textual representation.
     */
    @Override
    public String toString() {
        return "@[" + getSlot() + "," + getType() + "]";
    } // toString()
    
} // TupleSlotPointer
/*
 * Created on Dec 10, 2003 by sviglas
 *
 * Modified on Dec 26, 2008 by sviglas
 *
 * This is part of the attica project.  Any subsequent modification
 * of the file should retain this disclaimer.
 * 
 * University of Edinburgh, School of Informatics
 */


/**
 * TupleTupleCondition: A condition across tuples.
 *
 * @author sviglas
 */
class TupleTupleCondition implements Predicate {
	
    /** The pointer to the slot in the left tuple. */
    private TupleSlotPointer leftSlot;
    
    /** The left tuple. */
    private Tuple leftTuple;
	
    /** The pointer to the slot in the right tuple. */
    private TupleSlotPointer rightSlot;
	
    /** The right tuple. */
    private Tuple rightTuple;
    
    /** The qualification between the values. */
    private Condition.Qualification qualification;

    /**
     * Constructs a new condition across tuples.
     * 
     * @param leftSlot the pointer to the left-hand side tuple slot.
     * @param rightSlot the pointer to the right-hand side tuple slot.
     * @param qualification the qualification between the values of
     * the slots.
     */
    public TupleTupleCondition (TupleSlotPointer leftSlot, 
                                TupleSlotPointer rightSlot,
                                Condition.Qualification qualification) {
        this.leftSlot = leftSlot;
        this.rightSlot = rightSlot;
        this.qualification = qualification;
    } // TupleTupleCondition()
	
    /**
     * Sets the two tuples the predicate is to be evaluated over.
     * 
     * @param leftTuple the (new) left-hand side tuple.
     * @param rightTuple the (new) right-hand side tuple.
     */
    public void setTuples(Tuple leftTuple, Tuple rightTuple) {
        this.leftTuple = leftTuple;
        this.rightTuple = rightTuple;
    } // setTuples()

    
    /**
     * Implements the <code>Predicate</code> interface by defining the
     * </code>evaluate()</code> method.
     * 
     * @return <code>true</code> if the predicate evaluates to true,
     * <code>false</code> otherwise.
     */	
    public boolean evaluate() {
        Comparable leftValue = 
            (Comparable) leftTuple.getValue(leftSlot.getSlot());
        Comparable rightValue = 
            (Comparable) rightTuple.getValue(rightSlot.getSlot());
        return PredicateEvaluator.evaluate(new Condition(leftValue,
                                                         rightValue,
                                                         qualification));
    } // evaluate()

    
    /**
     * Textual representation.
     * 
     * @return the predicate's textual representation.
     */
    @Override
    public String toString() {
        return leftSlot + symbolString() + rightSlot;
    } // toString()

    
    /**
     * Convert the qualification to a string.
     * 
     * @return the symbol of the qualification as a string.
     */
    protected String symbolString() {
        switch (qualification) {
        case EQUALS: 
            return "=";
        case NOT_EQUALS: 
            return "!=";
        case GREATER:
            return ">";
        case LESS:
            return "<";
        case GREATER_EQUALS:
            return ">=";
        case LESS_EQUALS:
            return "<=";
        }
	
        return "?";
    } // symbolString()
	
} // TupleTupleCondition()
/*
 * Created on Dec 10, 2003 by sviglas
 *
 * Modified on Dec 26, 2008 by sviglas
 *
 * This is part of the attica project.  Any subsequent modification
 * of the file should retain this disclaimer.
 * 
 * University of Edinburgh, School of Informatics
 */


/**
 * TupleValueCondition: A condition on a tuple slot and a value.
 *
 * @author sviglas
 */
class TupleValueCondition implements Predicate {
	
    /** The pointer to the tuple slot used by the predicate. */
    private TupleSlotPointer leftSlot;
	
    /** The tuple the predicate is to be evaluated on. */
    private Tuple leftTuple;
    
    /** The <code>Comparable</code> right-hand side value. */
    private Comparable rightValue;
    
    /** The qualification between the tuple slot and the value. */
    private Condition.Qualification qualification;

    
    /**
     * Constructs a new <code>TupleValueCondition</code> given a pointer to a 
     * tuple slot and a <code>Comparable</code>.
     * 
     * @param leftSlot the pointer to the tuple slot
     * @param right the Java <code>Comparable</code> value used for
     * comparison.
     * @param qualification the qualification between the value of the
     * appropriate slot of the tuple and the <code>Comparable</code>
     * value.
     */
    public TupleValueCondition(TupleSlotPointer leftSlot,
                               Comparable right,
                               Condition.Qualification qualification) {
        this.leftSlot = leftSlot;
        rightValue = right;
        this.qualification = qualification;
    } // TupleValueCondition()

    
    /**
     * Sets the tuple this predicate is to be evaluated on.
     * 
     * @param leftTuple the tuple the predicate is to be evaluated on.
     */
    public void setTuple(Tuple leftTuple) {
        this.leftTuple = leftTuple;
    } // setTuple()

    
    /**
     * Implements the <code>Predicate</code> interface by defining
     * the </code>evaluate()</code> method.
     * 
     * @return <code>true</code> if the predicate evaluates to true,
     * <code>false</code> otherwise.
     */
    public boolean evaluate() {
        Comparable left = 
            (Comparable) leftTuple.getValue(leftSlot.getSlot());
        return PredicateEvaluator.evaluate(new Condition(left,
                                                         rightValue,
                                                         qualification));
    } // evaluate()

    
    /**
     * Textual representation.
     * 
     * @return the predicate's textual representation.
     */
    @Override
    public String toString() {
        return leftSlot + symbolString() + rightValue;
    } // toString()
	
	/**
	 * Convert the qualification to a string
	 * 
	 * @return the symbol of the qualification as a string
	 */
	protected String symbolString () {
		switch (qualification) {
			case EQUALS: 
				return "=";
			case NOT_EQUALS: 
				return "!=";
			case GREATER:
				return ">";
			case LESS:
				return "<";
			case GREATER_EQUALS:
				return ">=";
			case LESS_EQUALS:
				return "<=";
		}
	
		return "?";
	} // symbolString()

} // TupleValueCondition
/*
 * Created on Oct 5, 2003 by sviglas
 *
 * Modified on Dec 20, 2008 by sviglas
 *
 * This is part of the attica project.  Any subsequent modification
 * of the file should retain this disclaimer.
 * 
 * University of Edinburgh, School of Informatics
 */


/**
 * Attribute: The basic abstraction of an attica data model Attribute.
 *
 * @author sviglas
 */
class Attribute implements java.io.Serializable {
    
    /** The name of this attribute. */
    private String name;

    /** The type of this attribute. */
    private Class<? extends Comparable> type;

    
    /**
     * Create a new attribute given its name and class.
     * 
     * @param name the name of the attribute.
     * @param type the class of the attribute.
     */
    public Attribute(String name, Class<? extends Comparable> type) {
        this.name = name;
        this.type = type;
    } // Attribute()
    
		
    /**
     * Copy constructor for an attribute.
     * 
     * @param attribute the attribute to be copied.
     */
    public Attribute(Attribute attribute) {
        this.name = attribute.getName();
        this.type = attribute.getType();
    } // Attribute()

    
    /**
     * Return the name of this attribute.
     * 
     * @return this attribute's name.
     */
    public String getName() {
        return name;
    } // getName()

    
    /**
     * Return the class of this attribute.
     * 
     * @return this attribute's type.
     */
    public Class<? extends Comparable> getType() {
        return type;
    } // getType()

    public boolean equals(Object o) {
        if (this == o) return true;
        if (! (o instanceof Attribute)) return false;
        Attribute a = (Attribute) o;
        return getName().equals(a.getName()) && getType().equals(a.getType());
    }

    public int hashCode() {
        int hash = 17;
        hash = hash*31 + getName().hashCode();
        return hash*31 + getType().hashCode();
    }

    
    /**
     * Textual representation.
     */
    @Override
    public String toString() {
        return getName() + ":" + getType();
    } // toString()
    
} // Attribute
/*
 * Created on Oct 6, 2003 by sviglas
 *
 * Modified on Dec 20, 2008 by sviglas
 *
 * This is part of the attica project.  Any subsequent modification
 * of the file should retain this disclaimer.
 * 
 * University of Edinburgh, School of Informatics
 */


/**
 * Relation: The attica encapsulation of a relation schema.
 *
 * @author sviglas
 */
class Relation implements java.io.Serializable, Iterable<Attribute> {
    
    /** The attributes of this relation. */
    private List<Attribute> attributes;

    
    /** Default constructor for a relation. */
    public Relation() {
        attributes = new ArrayList<Attribute>();
    } // Relation()

    
    /**
     * Constructs a relation given a <code>List</code> of attributes.
     * 
     * @param attributes the attributes of the relation.
     */
    public Relation(List<Attribute> attributes) {
        this.attributes = new ArrayList<Attribute>(attributes);
    } // Relation()

    
    /**
     * Copy constructor for a relation.
     * 
     * @param relation the relation to be copied.
     */
    public Relation(Relation relation) {
        attributes = new ArrayList<Attribute>();
        for (Attribute attr : relation) 
            addAttribute(attr);
    } // Relation()


    /**
     * Adds an attribute to this relation.
     *
     * @param attribute the attribute to be added.
     */
    protected void addAttribute(Attribute attribute) {
        attributes.add(attribute);
    } // addAttribute()

    
    /**
     * Returns the number of attributes of this relation.
     * 
     * @return this <code>Relation</code>'s number of attributes
     */
    public int getNumberOfAttributes() {
        return attributes.size();
    } // getNumberOfAttributes()

    
    /**
     * Returns an <code>Iterator</code> over the attributes of this
     * relation.
     * 
     * @return this <code>Relation</code>'s attribute
     * <code>Iterator</code>.
     */
    public Iterator<Attribute> iterator() {
        return attributes.iterator();
    } // getAttributesIterator()
    

    /**
     * Returns the specified attribute of this relation.
     * 
     * @param i the index of the attribute to be returned.
     * @return the specified attribute.
     */
    public Attribute getAttribute(int i) {
        return attributes.get(i);
        //return ModelFactory.castAttribute(attributes[i]);
    } // getAttribute()

    
    /**
     * Given an attribute name, it returns the index of the attribute
     * in the relation schema. It returns <code>-1</code> if the
     * attribute does not appear in the schema.
     * 
     * @param attr the attribute name
     * @return the index of the attribute in the relation schema,
     * <code>-1</code> if the attribute does not appear in the schema.
     */
    public int getAttributeIndex(String attr) {        

        int i = 0;
        for (Attribute attribute : attributes)
            if (attribute.getName().equals(attr)) 
                return i;
        return -1;
    } // getAttributeIndex()


    /**
     * Textual representation.
     */
    @Override
    public String toString () {
        
        StringBuffer sb = new StringBuffer();
        sb.append("{");
        for (int i = 0; i < getNumberOfAttributes()-1; i++)
            sb.append(getAttribute(i) + ", ");
        sb.append(getAttribute(getNumberOfAttributes()-1) + "}");
        return sb.toString();
    } // toString()
    
} // Relation()
/*
 * Created on Oct 5, 2003 by sviglas
 *
 * Modified on Dec 22, 2008 by sviglas
 *
 * This is part of the attica project.  Any subsequent modification
 * of the file should retain this disclaimer.
 * 
 * University of Edinburgh, School of Informatics
 */


/**
 * Table: Encapsulates a <code>Table</code> in the attica data model.
 * A <code>Table</code> is nothing more than a named <code>Relation</code>.
 *
 * @author sviglas
 * @see Relation
 */
class Table extends Relation implements java.io.Serializable {
	
    /** This table's name. */
    private String name;
    
    /**
     * Constructs a new table given its name.
     * 
     * @param name the name of the <code>Table</code>.
     */
    public Table(String name) {
        super();
        this.name = name;
    } // Table()

    
    /**
     * Constructs a table given a name and <code>List</code> of
     * attributes.
     * 
     * @param name the name of the <code>Table</code>.
     * @param attributes the attributes of the <code>Table</code>.
     */
    public Table(String name, List<Attribute> attributes) {
        super(attributes);
        this.name = name;
    } // Table()

    
    /**
     * Retrieves the name of the table.
     * 
     * @return this <code>Table</code>'s name.
     */
    public String getName() {
        return name;
    } // getName()

} // Table
/*
 * Created on Dec 15, 2003 by sviglas
 *
 * Modified on Dec 22, 2008 by sviglas
 *
 * This is part of the attica project.  Any subsequent modification
 * of the file should retain this disclaimer.
 * 
 * University of Edinburgh, School of Informatics
 */

/**
 * TableAttribute: a qualified attribute that appears in a table
 * (wraps a standard attribute).
 *
 * @author sviglas
 */
class TableAttribute extends Attribute implements java.io.Serializable {
    
    /** The table of this attribute. */
    private String table;

    /**
     * Constructs a new table attribute given table, name and type.
     * 
     * @param table the table name.
     * @param name the name of the attribute.
     * @param type the type of the attribute.
     */
    public TableAttribute(String table, String name,
                          Class<? extends Comparable> type) {
        super(name, type);
        this.table = table;
    } // TableAttribute()
    
    
    /**
     * Copy constructor for table attributes.
     * 
     * @param ta the table attribute to be copied.
     */
    public TableAttribute(TableAttribute ta) {
        this(new String(ta.getTable()), 
             new String(ta.getName()), ta.getType()); 
    } // TableAttribute()
    
    
    /**
     * Retrieves the table of this attribute.
     * 
     * @return this attribute's table.
     */
    public String getTable() {
        return table;
    } // getTable()


    /**
     * Tests this attribute for equality to an object.
     *
     * @param o the object to test for equality.
     * @return <code>true</code> if the two objects are equal,
     * <code>false</code> otherwise.
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (! (o instanceof TableAttribute)) return false;
        TableAttribute a = (TableAttribute) o;
        return getName().equals(a.getName())
            && getType().equals(a.getType())
            && getTable().equals(a.getTable());
    }


    /**
     * Returns a hashcode for this attribute.
     *
     * @return a hash code for this attribute.
     */
    @Override
    public int hashCode() {
        int hash = 17;
        hash = hash*31 + getName().hashCode();        
        hash = hash*31 + getType().hashCode();        
        return hash*31 + getTable().hashCode();
    }
    
    
    /**
     * Textual representation.
     */
    @Override
    public String toString() {
        return getTable() + "." + getName() + ": " + getType();
    } // toString()
    
} // TableAttribute
/*
 * Created on Dec 14, 2003 by sviglas
 *
 * Modified on Dec 24, 2008 by sviglas
 *
 * This is part of the attica project. Any subsequent modification of
 * the file should retain this disclaimer.
 * 
 * University of Edinburgh, School of Informatics
 */











/**
 * Database: The database driver file.
 *
 * @author sviglas 
 */
class Database {
	
    /** The DB properties. */
    private Properties props;
	
    /** Directory of the server. */
    public static String ATTICA_DIR;
    
    /** Temporary directory. */
    public static String TEMP_DIR;
    
    /** The user prompt. */
    private static final String PROMPT = "aSQL > ";
    
    /** Number of pages in the buffer pool. */
    private int bufferSize;
    
    /** DB catalog. */
    private Catalog catalog;
    
    /** The storage manager instance. */
    private StorageManager sm;
	
    /** The buffer manager instance. */
    private BufferManager bm;
	
    /** SQL Parser. */
    private SQLParser parser = null;
	
    /**
     * Constructs a new Database instance using the given file name to
     * read the properties from.
     * 
     * @param propsFile the name of the file containing the DB
     * properties.
     * @throws ServerException thrown whenever the DB cannot be
     * constructed.
     */
    public Database(String propsFile) throws ServerException {
        this(propsFile, false);
    } // Database()
	
    /**
     * Constructs a new Database specifying whether this is a new
     * instance or not.
     * 
     * @param propsFile the name of the file containing the DB
     * properties.
     * @param init if set to <code>true</code> this is a new DB
     * instance, so the catalog is generated, if set to
     * <code>false</code> this is an old instance and the catalog is
     * simply read from disk.
     * @throws ServerException thrown whenever the DB cannot be
     * constructed.
     */
    public Database(String propsFile, boolean init) throws ServerException {
        
        try {
            System.out.println("Starting up the database...");
            System.out.println("Bootstrapping: Loading properties from "
                               + propsFile);
            FileInputStream fis = new FileInputStream(propsFile); 
            props = new Properties();
            props.load(fis); 
            fis.close(); 
            ATTICA_DIR = props.getProperty("attica.directory", 
                                           System.getProperty("user.dir")).trim();
            TEMP_DIR = props.getProperty("attica.temp.directory",
                                         System.getProperty("user.dir")).trim();
            bufferSize = 
	      Integer.parseInt(props.getProperty("attica.buffersize", "50").trim());
            
            // start up the new buffer manager
            bm = new BufferManager(bufferSize);
            // start up the catalog
            String catalogFile = ATTICA_DIR
                + System.getProperty("file.separator") + "attica.catalog";
            catalog = new Catalog(catalogFile);
            // don't look at me if you're stupid, blame your genes...
            if (init) catalog.writeCatalog();
            else catalog.readCatalog();
            // start up the storage manager
            sm = new StorageManager(catalog, bm);
			
            // we're all ready now...
            System.out.println("Attica server is running...");
            System.out.println("Data directory: " + ATTICA_DIR);
            System.out.println("Termporary directory: " + TEMP_DIR);
            System.out.println("Buffer pool size: " + bufferSize + " pages");
            System.out.println("** ready **");
            System.out.print(PROMPT);
        }
        catch (Exception e) {
            throw new ServerException("Could not initialise the database.", e);
        }
    } // Database()

    
    /**
     * Executes the given statement.
     * 
     * @param statement the statement to be executed.
     * @return a Sink operator for result retrieval.
     * @throws EngineException thrown whenever the statement cannot be 
     * executed.
     */
    public Sink runStatement(String statement) throws EngineException {
        
        // @#$%ing static parsers...
        if (parser == null) {
            //parser = new SQLParser(statement, catalog);
            parser =
                new SQLParser(new ByteArrayInputStream(statement.getBytes()));
            parser.setCatalog(catalog);
        }
        else {
            SQLParser.
                ReInit(new ByteArrayInputStream(statement.getBytes()));
        }
        
        Statement result = null;
		
        try {
            result = SQLParser.Start();
            // this is a query -- call the plan builder and run it
            if (result instanceof Query) {
                Query q = (Query) result;
                List<AlgebraicOperator> algebra = q.getAlgebra();
                PlanBuilder pb = new PlanBuilder(catalog, sm);
                String file = FileUtil.createTempFileName();
                Operator operator = pb.buildPlan(file, algebra);
                return (Sink) operator;
            }
            // this is a new table -- insert it into the database
            else if (result instanceof TableCreation) {
                TableCreation tc = (TableCreation) result;
                Table table = tc.getTable();
                sm.createTable(table);
                catalog.writeCatalog();
                MessageSink ms = new MessageSink("Table " + table.getName() +
                                                 " successfully created");
                return ms;
            }
            // this is a drop
            else if (result instanceof TableDeletion) {
                TableDeletion td = (TableDeletion) result;
                String tablename = td.getTableName();
                sm.deleteTable(tablename);
                MessageSink ms = new MessageSink("Table " + tablename +
                                                 " successfully dropped");
                return ms;
            }
            // this is a tuple insertion -- make it happen
            else if (result instanceof TupleInsertion) {
                TupleInsertion ti = (TupleInsertion) result;
                String table = ti.getTableName();
                List<Comparable> values = ti.getValues();
                sm.castAndInsertTuple(table, values);
                MessageSink ms = new MessageSink("Tuple was successfully "
                                                 + "inserted into table "
                                                 + table);
                return ms;
            }
            // show the DB catalog
            else if (result instanceof ShowCatalog) {
                MessageSink ms = new MessageSink(catalog.toString());
                return ms;
            }
            // show the table attributes
            else if (result instanceof TableDescription) {
                TableDescription td = (TableDescription) result;
                String name = td.getTableName();
                Table table = catalog.getTable(name);
                MessageSink ms = new MessageSink(table.toString());
                return ms;
            }
        }
        catch (Exception e) {
            MessageSink ms = new MessageSink(e.getMessage());
            return ms;
        }
		
        MessageSink ms = new MessageSink("Could not run query");
        return ms;
    } // runQuery()

    
    /**
     * Shuts down the server.
     * 
     * @throws ServerException thrown whenever the server cannot be shut
     * down.
     */
    public void shutdown() throws ServerException {
        
        try {
            // dump the catalog
            catalog.writeCatalog();
            // shut down the storage manager -- it'll flush the buffer
            // pool too; I know this is not the best encapsulation
            // ever, but de-coupling them at this point will probably
            // cause a rewrite of parts of the pool and the storage
            // manager that I don't want to touch right now. (by the
            // way, what the hell was I thinking five years ago?)
            sm.shutdown();
        }
        catch (StorageManagerException sme) {
            throw new ServerException("Could not shut down the server", sme);
        }    
    } // shutDown()

    
    /**
     * Start up the database.
     * 
     * @param args the arguments to the server.
     */
    public static void main (String [] args) {
        
        try {
            // parse arguments
            if (Args.gettrig(args, "--help")) {
                System.err.println("Usage: java attica.server.Database "
                                   + "<attica-properties-file> [--init]");
                System.exit(0);
            }

            // load the properties
            String properties =
                Args.getopt(args, "--properties", "attica.properties");
            // should we initialise the database?
            boolean init = Args.gettrig(args, "--init");
            // construct the db instance
            Database db = new Database(properties, init);
            // the input stream
            Reader in = new InputStreamReader(System.in);

            // and now the simplest prompt loop in the history of
            // database prompt loops. actually, simple doesn't quite
            // describe it. some people might even call it lame. I
            // would agree with those people. I do understand that I'm
            // calling myself lame, but at least I have the
            // self-knowledge to do so. so let me repeat: this is
            // lame. in fact, if you have a minute, it is an eight
            // storey high temple of lameness, with a big flashing
            // neon sign on top saying "this is lame".
            boolean done = false;
            while (! done) {
                StringBuffer sb = new StringBuffer();
                char c;
                // yeah, so we need to call a method just to append a
                // character... isn't java lovely?
                while ((c = (char) in.read()) != ';') sb.append(c);
                
                String input = sb.toString();
                done = input.trim().equals("exit");
                if (! done) {
                    Sink sink = db.runStatement(input);

                    for (Tuple tuple : sink.tuples())
                        System.out.println(tuple.toStringFormatted());

                    System.out.print(PROMPT);
                }
            }
            db.shutdown();
        }
        catch (Exception e) {
            System.err.println(e.getMessage());
            e.printStackTrace(System.err);
        }
    } // main()

} // Database
/*
 * Created on Dec 26, 2003 by sviglas
 *
 * Modified on Dec 27, 2008 by sviglas
 *
 * This is part of the attica project.  Any subsequent modification
 * of the file should retain this disclaimer.
 * 
 * University of Edinburgh, School of Informatics
 */



/**
 * Query: Encapsulates a user query.
 *
 * @author sviglas
 */
class Query extends Statement {
	
    /** The algebra for this query. */
    private List<AlgebraicOperator> algebra;
    
    /**
     * Constructs a new query.
     * 
     * @param algebra the algebra for the query.
     */
    public Query(List<AlgebraicOperator> algebra) {
        this.algebra = algebra;
    } // Query()

    
    /**
     * Retrieve the algebra for this query.
     * 
     * @return this query's algebra.
     */
    public List<AlgebraicOperator> getAlgebra() {
        return algebra;
    } // getAlgebra()
} // Statement
/*
 * Created on Dec 14, 2003 by sviglas
 *
 * This is part of the attica project.  Any subsequent modification
 * of the file should retain this disclaimer.
 * 
 * University of Edinburgh, School of Informatics
 */

/**
 * ServerException: The basic exception thrown for all DB server
 * errors.
 *
 * @author sviglas
 */
class ServerException extends Exception {
	
    /**
     * Constructs a new server exception.
     * 
     * @param msg this server exception's message.
     */
    public ServerException(String msg) {
        super(msg);
    } // ServerException()

    
    /**
     * Constructs a new server exception given a message and
     * originating throwable.
     *
     * @param msg the error message.
     * @param e the originating throwable.
     */
    public ServerException(String msg, Throwable e) {
        super(msg, e);
    } // ServerException()

} // ServerException
/*
 * Created on Dec 26, 2003 by sviglas
 *
 * This is part of the attica project.  Any subsequent modification
 * of the file should retain this disclaimer.
 * 
 * University of Edinburgh, School of Informatics
 */

/**
 * ShowCatalog: Dumps the system catalog.
 *
 * @author sviglas
 */
class ShowCatalog extends Statement {
	
    /**
     * Default constructor.
     */
    public ShowCatalog () {
    } // ShowCatalog()

} // ShowCatalog
/*
 * Created on Dec 26, 2003 by sviglas
 *
 * This is part of the attica project.  Any subsequent modification
 * of the file should retain this disclaimer.
 * 
 * University of Edinburgh, School of Informatics
 */

/**
 * Command: Superclass of all user statements.
 *
 * @author sviglas
 */
abstract class Statement {
	
    /**
     * Constructs a new statement.
     */
    public Statement() {
    } // Statement()

} // Statement
/*
 * Created on Dec 26, 2003 by sviglas
 *
 * This is part of the attica project.  Any subsequent modification
 * of the file should retain this disclaimer.
 * 
 * University of Edinburgh, School of Informatics
 */


/**
 * TableCreation: Encapsulates a table creation command.
 *
 * @author sviglas
 */
class TableCreation extends Statement {
	
    /** The description of the table to be created. */
    private Table table;

    
    /**
     * Constructs a new table creation command.
     * 
     * @param table the declaration of the table to be constructed.
     */
    public TableCreation(Table table) {
        this.table = table;
    } // TableCreation()

    
    /**
     * Retrieves the table to be created.
     * 
     * @return the table to be created.
     */
    public Table getTable() { 
        return table;
    } // getTable()

} // TableCreation
/*
 * Created on Dec 26, 2003 by sviglas
 *
 * This is part of the attica project.  Any subsequent modification
 * of the file should retain this disclaimer.
 * 
 * University of Edinburgh, School of Informatics
 */

/**
 * TableDeletion: Encapsulates a table deletion command.
 *
 * @author sviglas
 */
class TableDeletion extends Statement {

    /** The name of the table. */
    private String tablename;

    
    /**
     * Constructs a table deletion statement.
     * 
     * @param tablename the name of the table to be deleted.
     */
    public TableDeletion(String tablename) {
        this.tablename = tablename;
    } // TableDeletion()

    
    /**
     * Retrieves the name of the table that is to be deleted.
     * 
     * @return the table to be deleted.
     */
    public String getTableName() {
        return tablename;
    } // getTableName()
	
} // TableDeletion
/*
 * Created on Dec 26, 2003 by sviglas
 *
 * This is part of the attica project.  Any subsequent modification
 * of the file should retain this disclaimer.
 * 
 * University of Edinburgh, School of Informatics
 */

/**
 * TableDescription: A table description command.
 *
 * @author sviglas
 */
class TableDescription extends Statement {
	
    /** The name of the table to be described */
    private String tablename;

    
    /**
     * Constructs a new table description command.
     * 
     * @param tablename the name of the table to be described.
     */
    public TableDescription(String tablename) {
        this.tablename = tablename;
    } // TableDescription()

    
    /**
     * Retrieves the name of the table to be described.
     * 
     * @return the table to be described.
     */
    public String getTableName() {
        return tablename;
    } // getTableName()

} // TableDescription()
/*
 * Created on Dec 26, 2003 by sviglas
 *
 * This is part of the attica project.  Any subsequent modification
 * of the file should retain this disclaimer.
 * 
 * University of Edinburgh, School of Informatics
 */


/**
 * TupleInsertion: A new tuple insertion.
 *
 * @author sviglas
 */
class TupleInsertion extends Statement {
    
    /** The name of the table where the tuple is to be inserted. */
    private String table;
	
    /** The list of values. */
    private List<Comparable> values;

    
    /**
     * Constructs a new tuple insertion command.
     * 
     * @param table the table where the tuples should be inserted.
     * @param values the values of the tuple.
     */
    public TupleInsertion(String table, List<Comparable> values) {
        this.table = table;
        this.values = values;
    } // TupleInsertion()

    
    /**
     * Returns the table where the tuple is to be inserted.
     * 
     * @return the table where the tuple is to be inserted.
     */
    public String getTableName() {
        return table;
    } // getTableName()

    
    /**
     * Retrieves the values of the tuple.
     * 
     * @return the tuple's values.
     */
    public List<Comparable> getValues() {
        return values;
    } // getValues()

} // TupleInsertion
/* Generated By:JavaCC: Do not edit this line. ParseException.java Version 3.0 */

/**
 * This exception is thrown when parse errors are encountered.
 * You can explicitly create objects of this exception type by
 * calling the method generateParseException in the generated
 * parser.
 *
 * You can modify this class to customize your error reporting
 * mechanisms so long as you retain the public fields.
 */
class ParseException extends Exception {

  /**
   * This constructor is used by the method "generateParseException"
   * in the generated parser.  Calling this constructor generates
   * a new object of this type with the fields "currentToken",
   * "expectedTokenSequences", and "tokenImage" set.  The boolean
   * flag "specialConstructor" is also set to true to indicate that
   * this constructor was used to create this object.
   * This constructor calls its super class with the empty string
   * to force the "toString" method of parent class "Throwable" to
   * print the error message in the form:
   *     ParseException: <result of getMessage>
   */
  public ParseException(Token currentTokenVal,
                        int[][] expectedTokenSequencesVal,
                        String[] tokenImageVal
                       )
  {
    super("");
    specialConstructor = true;
    currentToken = currentTokenVal;
    expectedTokenSequences = expectedTokenSequencesVal;
    tokenImage = tokenImageVal;
  }

  /**
   * The following constructors are for use by you for whatever
   * purpose you can think of.  Constructing the exception in this
   * manner makes the exception behave in the normal way - i.e., as
   * documented in the class "Throwable".  The fields "errorToken",
   * "expectedTokenSequences", and "tokenImage" do not contain
   * relevant information.  The JavaCC generated code does not use
   * these constructors.
   */

  public ParseException() {
    super();
    specialConstructor = false;
  }

  public ParseException(String message) {
    super(message);
    specialConstructor = false;
  }

  /**
   * This variable determines which constructor was used to create
   * this object and thereby affects the semantics of the
   * "getMessage" method (see below).
   */
  protected boolean specialConstructor;

  /**
   * This is the last token that has been consumed successfully.  If
   * this object has been created due to a parse error, the token
   * followng this token will (therefore) be the first error token.
   */
  public Token currentToken;

  /**
   * Each entry in this array is an array of integers.  Each array
   * of integers represents a sequence of tokens (by their ordinal
   * values) that is expected at this point of the parse.
   */
  public int[][] expectedTokenSequences;

  /**
   * This is a reference to the "tokenImage" array of the generated
   * parser within which the parse error occurred.  This array is
   * defined in the generated ...Constants interface.
   */
  public String[] tokenImage;

  /**
   * This method has the standard behavior when this object has been
   * created using the standard constructors.  Otherwise, it uses
   * "currentToken" and "expectedTokenSequences" to generate a parse
   * error message and returns it.  If this object has been created
   * due to a parse error, and you do not catch it (it gets thrown
   * from the parser), then this method is called during the printing
   * of the final stack trace, and hence the correct error message
   * gets displayed.
   */
  public String getMessage() {
    if (!specialConstructor) {
      return super.getMessage();
    }
    StringBuffer expected = new StringBuffer();
    int maxSize = 0;
    for (int i = 0; i < expectedTokenSequences.length; i++) {
      if (maxSize < expectedTokenSequences[i].length) {
        maxSize = expectedTokenSequences[i].length;
      }
      for (int j = 0; j < expectedTokenSequences[i].length; j++) {
        expected.append(tokenImage[expectedTokenSequences[i][j]]).append(" ");
      }
      if (expectedTokenSequences[i][expectedTokenSequences[i].length - 1] != 0) {
        expected.append("...");
      }
      expected.append(eol).append("    ");
    }
    String retval = "Encountered \"";
    Token tok = currentToken.next;
    for (int i = 0; i < maxSize; i++) {
      if (i != 0) retval += " ";
      if (tok.kind == 0) {
        retval += tokenImage[0];
        break;
      }
      retval += add_escapes(tok.image);
      tok = tok.next; 
    }
    retval += "\" at line " + currentToken.next.beginLine + ", column " + currentToken.next.beginColumn;
    retval += "." + eol;
    if (expectedTokenSequences.length == 1) {
      retval += "Was expecting:" + eol + "    ";
    } else {
      retval += "Was expecting one of:" + eol + "    ";
    }
    retval += expected.toString();
    return retval;
  }

  /**
   * The end of line string for this machine.
   */
  protected String eol = System.getProperty("line.separator", "\n");
 
  /**
   * Used to convert raw characters to their escaped version
   * when these raw version cannot be used as part of an ASCII
   * string literal.
   */
  protected String add_escapes(String str) {
      StringBuffer retval = new StringBuffer();
      char ch;
      for (int i = 0; i < str.length(); i++) {
        switch (str.charAt(i))
        {
           case 0 :
              continue;
           case '\b':
              retval.append("\\b");
              continue;
           case '\t':
              retval.append("\\t");
              continue;
           case '\n':
              retval.append("\\n");
              continue;
           case '\f':
              retval.append("\\f");
              continue;
           case '\r':
              retval.append("\\r");
              continue;
           case '\"':
              retval.append("\\\"");
              continue;
           case '\'':
              retval.append("\\\'");
              continue;
           case '\\':
              retval.append("\\\\");
              continue;
           default:
              if ((ch = str.charAt(i)) < 0x20 || ch > 0x7e) {
                 String s = "0000" + Integer.toString(ch, 16);
                 retval.append("\\u" + s.substring(s.length() - 4, s.length()));
              } else {
                 retval.append(ch);
              }
              continue;
        }
      }
      return retval.toString();
   }

}
/* Generated By:JavaCC: Do not edit this line. SimpleCharStream.java Version 4.0 */

/**
 * An implementation of interface CharStream, where the stream is assumed to
 * contain only ASCII characters (without unicode processing).
 */

class SimpleCharStream
{
  public static final boolean staticFlag = true;
  static int bufsize;
  static int available;
  static int tokenBegin;
  static public int bufpos = -1;
  static protected int bufline[];
  static protected int bufcolumn[];

  static protected int column = 0;
  static protected int line = 1;

  static protected boolean prevCharIsCR = false;
  static protected boolean prevCharIsLF = false;

  static protected java.io.Reader inputStream;

  static protected char[] buffer;
  static protected int maxNextCharInd = 0;
  static protected int inBuf = 0;
  static protected int tabSize = 8;

  static protected void setTabSize(int i) { tabSize = i; }
  static protected int getTabSize(int i) { return tabSize; }


  static protected void ExpandBuff(boolean wrapAround)
  {
     char[] newbuffer = new char[bufsize + 2048];
     int newbufline[] = new int[bufsize + 2048];
     int newbufcolumn[] = new int[bufsize + 2048];

     try
     {
        if (wrapAround)
        {
           System.arraycopy(buffer, tokenBegin, newbuffer, 0, bufsize - tokenBegin);
           System.arraycopy(buffer, 0, newbuffer,
                                             bufsize - tokenBegin, bufpos);
           buffer = newbuffer;

           System.arraycopy(bufline, tokenBegin, newbufline, 0, bufsize - tokenBegin);
           System.arraycopy(bufline, 0, newbufline, bufsize - tokenBegin, bufpos);
           bufline = newbufline;

           System.arraycopy(bufcolumn, tokenBegin, newbufcolumn, 0, bufsize - tokenBegin);
           System.arraycopy(bufcolumn, 0, newbufcolumn, bufsize - tokenBegin, bufpos);
           bufcolumn = newbufcolumn;

           maxNextCharInd = (bufpos += (bufsize - tokenBegin));
        }
        else
        {
           System.arraycopy(buffer, tokenBegin, newbuffer, 0, bufsize - tokenBegin);
           buffer = newbuffer;

           System.arraycopy(bufline, tokenBegin, newbufline, 0, bufsize - tokenBegin);
           bufline = newbufline;

           System.arraycopy(bufcolumn, tokenBegin, newbufcolumn, 0, bufsize - tokenBegin);
           bufcolumn = newbufcolumn;

           maxNextCharInd = (bufpos -= tokenBegin);
        }
     }
     catch (Throwable t)
     {
        throw new Error(t.getMessage());
     }


     bufsize += 2048;
     available = bufsize;
     tokenBegin = 0;
  }

  static protected void FillBuff() throws java.io.IOException
  {
     if (maxNextCharInd == available)
     {
        if (available == bufsize)
        {
           if (tokenBegin > 2048)
           {
              bufpos = maxNextCharInd = 0;
              available = tokenBegin;
           }
           else if (tokenBegin < 0)
              bufpos = maxNextCharInd = 0;
           else
              ExpandBuff(false);
        }
        else if (available > tokenBegin)
           available = bufsize;
        else if ((tokenBegin - available) < 2048)
           ExpandBuff(true);
        else
           available = tokenBegin;
     }

     int i;
     try {
        if ((i = inputStream.read(buffer, maxNextCharInd,
                                    available - maxNextCharInd)) == -1)
        {
           inputStream.close();
           throw new java.io.IOException();
        }
        else
           maxNextCharInd += i;
        return;
     }
     catch(java.io.IOException e) {
        --bufpos;
        backup(0);
        if (tokenBegin == -1)
           tokenBegin = bufpos;
        throw e;
     }
  }

  static public char BeginToken() throws java.io.IOException
  {
     tokenBegin = -1;
     char c = readChar();
     tokenBegin = bufpos;

     return c;
  }

  static protected void UpdateLineColumn(char c)
  {
     column++;

     if (prevCharIsLF)
     {
        prevCharIsLF = false;
        line += (column = 1);
     }
     else if (prevCharIsCR)
     {
        prevCharIsCR = false;
        if (c == '\n')
        {
           prevCharIsLF = true;
        }
        else
           line += (column = 1);
     }

     switch (c)
     {
        case '\r' :
           prevCharIsCR = true;
           break;
        case '\n' :
           prevCharIsLF = true;
           break;
        case '\t' :
           column--;
           column += (tabSize - (column % tabSize));
           break;
        default :
           break;
     }

     bufline[bufpos] = line;
     bufcolumn[bufpos] = column;
  }

  static public char readChar() throws java.io.IOException
  {
     if (inBuf > 0)
     {
        --inBuf;

        if (++bufpos == bufsize)
           bufpos = 0;

        return buffer[bufpos];
     }

     if (++bufpos >= maxNextCharInd)
        FillBuff();

     char c = buffer[bufpos];

     UpdateLineColumn(c);
     return (c);
  }

  /**
   * @deprecated 
   * @see #getEndColumn
   */

  static public int getColumn() {
     return bufcolumn[bufpos];
  }

  /**
   * @deprecated 
   * @see #getEndLine
   */

  static public int getLine() {
     return bufline[bufpos];
  }

  static public int getEndColumn() {
     return bufcolumn[bufpos];
  }

  static public int getEndLine() {
     return bufline[bufpos];
  }

  static public int getBeginColumn() {
     return bufcolumn[tokenBegin];
  }

  static public int getBeginLine() {
     return bufline[tokenBegin];
  }

  static public void backup(int amount) {

    inBuf += amount;
    if ((bufpos -= amount) < 0)
       bufpos += bufsize;
  }

  public SimpleCharStream(java.io.Reader dstream, int startline,
  int startcolumn, int buffersize)
  {
    if (inputStream != null)
       throw new Error("\n   ERROR: Second call to the constructor of a static SimpleCharStream.  You must\n" +
       "       either use ReInit() or set the JavaCC option STATIC to false\n" +
       "       during the generation of this class.");
    inputStream = dstream;
    line = startline;
    column = startcolumn - 1;

    available = bufsize = buffersize;
    buffer = new char[buffersize];
    bufline = new int[buffersize];
    bufcolumn = new int[buffersize];
  }

  public SimpleCharStream(java.io.Reader dstream, int startline,
                          int startcolumn)
  {
     this(dstream, startline, startcolumn, 4096);
  }

  public SimpleCharStream(java.io.Reader dstream)
  {
     this(dstream, 1, 1, 4096);
  }
  public void ReInit(java.io.Reader dstream, int startline,
  int startcolumn, int buffersize)
  {
    inputStream = dstream;
    line = startline;
    column = startcolumn - 1;

    if (buffer == null || buffersize != buffer.length)
    {
      available = bufsize = buffersize;
      buffer = new char[buffersize];
      bufline = new int[buffersize];
      bufcolumn = new int[buffersize];
    }
    prevCharIsLF = prevCharIsCR = false;
    tokenBegin = inBuf = maxNextCharInd = 0;
    bufpos = -1;
  }

  public void ReInit(java.io.Reader dstream, int startline,
                     int startcolumn)
  {
     ReInit(dstream, startline, startcolumn, 4096);
  }

  public void ReInit(java.io.Reader dstream)
  {
     ReInit(dstream, 1, 1, 4096);
  }
  public SimpleCharStream(java.io.InputStream dstream, String encoding, int startline,
  int startcolumn, int buffersize) throws java.io.UnsupportedEncodingException
  {
     this(encoding == null ? new java.io.InputStreamReader(dstream) : new java.io.InputStreamReader(dstream, encoding), startline, startcolumn, buffersize);
  }

  public SimpleCharStream(java.io.InputStream dstream, int startline,
  int startcolumn, int buffersize)
  {
     this(new java.io.InputStreamReader(dstream), startline, startcolumn, buffersize);
  }

  public SimpleCharStream(java.io.InputStream dstream, String encoding, int startline,
                          int startcolumn) throws java.io.UnsupportedEncodingException
  {
     this(dstream, encoding, startline, startcolumn, 4096);
  }

  public SimpleCharStream(java.io.InputStream dstream, int startline,
                          int startcolumn)
  {
     this(dstream, startline, startcolumn, 4096);
  }

  public SimpleCharStream(java.io.InputStream dstream, String encoding) throws java.io.UnsupportedEncodingException
  {
     this(dstream, encoding, 1, 1, 4096);
  }

  public SimpleCharStream(java.io.InputStream dstream)
  {
     this(dstream, 1, 1, 4096);
  }

  public void ReInit(java.io.InputStream dstream, String encoding, int startline,
                          int startcolumn, int buffersize) throws java.io.UnsupportedEncodingException
  {
     ReInit(encoding == null ? new java.io.InputStreamReader(dstream) : new java.io.InputStreamReader(dstream, encoding), startline, startcolumn, buffersize);
  }

  public void ReInit(java.io.InputStream dstream, int startline,
                          int startcolumn, int buffersize)
  {
     ReInit(new java.io.InputStreamReader(dstream), startline, startcolumn, buffersize);
  }

  public void ReInit(java.io.InputStream dstream, String encoding) throws java.io.UnsupportedEncodingException
  {
     ReInit(dstream, encoding, 1, 1, 4096);
  }

  public void ReInit(java.io.InputStream dstream)
  {
     ReInit(dstream, 1, 1, 4096);
  }
  public void ReInit(java.io.InputStream dstream, String encoding, int startline,
                     int startcolumn) throws java.io.UnsupportedEncodingException
  {
     ReInit(dstream, encoding, startline, startcolumn, 4096);
  }
  public void ReInit(java.io.InputStream dstream, int startline,
                     int startcolumn)
  {
     ReInit(dstream, startline, startcolumn, 4096);
  }
  static public String GetImage()
  {
     if (bufpos >= tokenBegin)
        return new String(buffer, tokenBegin, bufpos - tokenBegin + 1);
     else
        return new String(buffer, tokenBegin, bufsize - tokenBegin) +
                              new String(buffer, 0, bufpos + 1);
  }

  static public char[] GetSuffix(int len)
  {
     char[] ret = new char[len];

     if ((bufpos + 1) >= len)
        System.arraycopy(buffer, bufpos - len + 1, ret, 0, len);
     else
     {
        System.arraycopy(buffer, bufsize - (len - bufpos - 1), ret, 0,
                                                          len - bufpos - 1);
        System.arraycopy(buffer, 0, ret, len - bufpos - 1, bufpos + 1);
     }

     return ret;
  }

  static public void Done()
  {
     buffer = null;
     bufline = null;
     bufcolumn = null;
  }

  /**
   * Method to adjust line and column numbers for the start of a token.
   */
  static public void adjustBeginLineColumn(int newLine, int newCol)
  {
     int start = tokenBegin;
     int len;

     if (bufpos >= tokenBegin)
     {
        len = bufpos - tokenBegin + inBuf + 1;
     }
     else
     {
        len = bufsize - tokenBegin + bufpos + 1 + inBuf;
     }

     int i = 0, j = 0, k = 0;
     int nextColDiff = 0, columnDiff = 0;

     while (i < len &&
            bufline[j = start % bufsize] == bufline[k = ++start % bufsize])
     {
        bufline[j] = newLine;
        nextColDiff = columnDiff + bufcolumn[k] - bufcolumn[j];
        bufcolumn[j] = newCol + columnDiff;
        columnDiff = nextColDiff;
        i++;
     } 

     if (i < len)
     {
        bufline[j] = newLine++;
        bufcolumn[j] = newCol + columnDiff;

        while (i++ < len)
        {
           if (bufline[j = start % bufsize] != bufline[++start % bufsize])
              bufline[j] = newLine++;
           else
              bufline[j] = newLine;
        }
     }

     line = bufline[j];
     column = bufcolumn[j];
  }

}
/* Generated By:JavaCC: Do not edit this line. SQLParser.java */




class SQLParser implements SQLParserConstants {

       private Catalog catalog;

        public void setCatalog(Catalog cat) {
               this.catalog = cat;
        }

        public static void main(String args[]) {
                System.out.println("Reading from standard input...");
                SQLParser t = new SQLParser(System.in);
                try {
                        t.Start();
                        System.out.println("Thank you.");
                } catch (Exception e) {
                        System.out.println("Oops.");
                        System.out.println(e.getMessage());
                        e.printStackTrace();
                }
        }

/*
TOKEN:
{
	< EMPTY: "" >
}
*/

/****************************************************
 ** The SQL grammar starts from this point forward **
 ****************************************************/
  static final public Statement Start() throws ParseException {
        Object o = null;
    switch ((jj_ntk==-1)?jj_ntk():jj_ntk) {
    case SELECT:
      o = Query();
                                // safe to ignore the warning since we are
                                // casting to the return type
                                @SuppressWarnings("unchecked")
                                List<AlgebraicOperator> ops =
                                        (List<AlgebraicOperator>) o;
                                {if (true) return new Query(ops);}
      break;
    case CREATE:
      o = Create();
                                {if (true) return new TableCreation((Table) o);}
      break;
    case DROP:
      o = Drop();
                                {if (true) return new TableDeletion((String) o);}
      break;
    case INSERT:
      o = Insert();
                                // safe to ignore the warning since we are
                                // casting to the return type
                                @SuppressWarnings("unchecked")
                                Pair<String, List<Comparable>> pair =
                                     (Pair<String, List<Comparable>>) o;
                                {if (true) return new TupleInsertion(pair.first,
                                                          pair.second);}
      break;
    case CATALOG:
      Catalog();
                                {if (true) return new ShowCatalog();}
      break;
    case DESCRIBE:
      o = Describe();
                                {if (true) return new TableDescription((String) o);}
      break;
    default:
      jj_la1[0] = jj_gen;
      jj_consume_token(-1);
      throw new ParseException();
    }
    throw new Error("Missing return statement in function");
  }

  static final public List<AlgebraicOperator> Query() throws ParseException {
        List<AlgebraicOperator> algebra = new ArrayList<AlgebraicOperator>();
        List<AlgebraicOperator> where = null;
        Projection p = null;
        Sort s = null;
    p = SelectClause();
                        algebra.add(p);
    FromClause();
    switch ((jj_ntk==-1)?jj_ntk():jj_ntk) {
    case WHERE:
      where = WhereClause();
                                algebra.addAll(where);
      break;
    default:
      jj_la1[1] = jj_gen;
      ;
    }
    switch ((jj_ntk==-1)?jj_ntk():jj_ntk) {
    case ORDER:
      s = SortClause();
                                algebra.add(s);
      break;
    default:
      jj_la1[2] = jj_gen;
      ;
    }
                        {if (true) return algebra;}
    throw new Error("Missing return statement in function");
  }

  static final public Projection SelectClause() throws ParseException {
        List<Variable> projections = new ArrayList<Variable>();
    jj_consume_token(SELECT);
    projections = AttributeList();
                        //System.out.println(projections);
                        Projection p = new Projection(projections);
                        {if (true) return p;}
    throw new Error("Missing return statement in function");
  }

  static final public void FromClause() throws ParseException {
    jj_consume_token(FROM);
    TableList();
  }

  static final public List<AlgebraicOperator> WhereClause() throws ParseException {
        List<AlgebraicOperator> v = null;
    jj_consume_token(WHERE);
    v = BooleanExpression();
                        {if (true) return v;}
    throw new Error("Missing return statement in function");
  }

  static final public Sort SortClause() throws ParseException {
        List<Variable> attributes = new ArrayList<Variable>();
    jj_consume_token(ORDER);
    jj_consume_token(BY);
    attributes = AttributeList();
                        Sort s = new Sort(attributes);
                        {if (true) return s;}
    throw new Error("Missing return statement in function");
  }

  static final public List<Variable> AttributeList() throws ParseException {
        List<Variable> v = new ArrayList<Variable>();
        Variable var = null;
    var = Attribute();
                        v.add(var);
    label_1:
    while (true) {
      switch ((jj_ntk==-1)?jj_ntk():jj_ntk) {
      case COMMA:
        ;
        break;
      default:
        jj_la1[3] = jj_gen;
        break label_1;
      }
      jj_consume_token(COMMA);
      var = Attribute();
                        v.add(var);
    }
                        {if (true) return v;}
    throw new Error("Missing return statement in function");
  }

  static final public void TableList() throws ParseException {
    Table();
    label_2:
    while (true) {
      switch ((jj_ntk==-1)?jj_ntk():jj_ntk) {
      case COMMA:
        ;
        break;
      default:
        jj_la1[4] = jj_gen;
        break label_2;
      }
      jj_consume_token(COMMA);
      Table();
    }
  }

  static final public String Table() throws ParseException {
        String x = null;
    if (jj_2_1(2147483647)) {
      AliasedTable();
                                {if (true) throw new ParseException("Table aliases not "
                                                         + "yet supported.");}
    } else {
      switch ((jj_ntk==-1)?jj_ntk():jj_ntk) {
      case ID:
        x = Identifier();
                                {if (true) return x;}
        break;
      default:
        jj_la1[5] = jj_gen;
        jj_consume_token(-1);
        throw new ParseException();
      }
    }
    throw new Error("Missing return statement in function");
  }

  static final public Variable Attribute() throws ParseException {
        Variable var = null;
    if (jj_2_2(2147483647)) {
      var = QualifiedAttribute();
                                {if (true) return var;}
    } else {
      switch ((jj_ntk==-1)?jj_ntk():jj_ntk) {
      case ID:
        Identifier();
                                {if (true) throw new ParseException("Unqualified "
                                                         + "attributes not "
                                                         + "yet supported.");}
        break;
      default:
        jj_la1[6] = jj_gen;
        jj_consume_token(-1);
        throw new ParseException();
      }
    }
    throw new Error("Missing return statement in function");
  }

  static final public List<AlgebraicOperator> BooleanExpression() throws ParseException {
        List<AlgebraicOperator> v = new ArrayList<AlgebraicOperator>();
    v = DisjunctiveExpression();
                        {if (true) return v;}
    throw new Error("Missing return statement in function");
  }

  static final public List<AlgebraicOperator> DisjunctiveExpression() throws ParseException {
        List<AlgebraicOperator> v = new ArrayList<AlgebraicOperator>();
    v = ConjunctiveExpression();
    label_3:
    while (true) {
      if (jj_2_3(2147483647)) {
        ;
      } else {
        break label_3;
      }
      DisjunctionOperator();
                                        {if (true) throw new ParseException("Disjunction "
                                                                 + "not yet "
                                                                 + "supported");}
      ConjunctiveExpression();
    }
                        {if (true) return v;}
    throw new Error("Missing return statement in function");
  }

  static final public List<AlgebraicOperator> ConjunctiveExpression() throws ParseException {
        List<AlgebraicOperator> algebra = new ArrayList<AlgebraicOperator>();
        AlgebraicOperator op = null;
    op = UnaryExpression();
                        algebra.add(op);
    label_4:
    while (true) {
      if (jj_2_4(2147483647)) {
        ;
      } else {
        break label_4;
      }
      ConjunctionOperator();
      op = UnaryExpression();
                                        algebra.add(op);
    }
                        {if (true) return algebra;}
    throw new Error("Missing return statement in function");
  }

  static final public AlgebraicOperator UnaryExpression() throws ParseException {
        AlgebraicOperator op = null;
    switch ((jj_ntk==-1)?jj_ntk():jj_ntk) {
    case NOT:
      NegationOperator();
      BooleanExpression();
                                {if (true) throw new ParseException("Negation not yet "
                                                         + "supported.");}
      break;
    case OPENPAR:
      jj_consume_token(OPENPAR);
      BooleanExpression();
      jj_consume_token(CLOSEPAR);
                                {if (true) throw new ParseException("Nested expressions "
                                                         + "not yet "
                                                         + "supported.");}
      break;
    case ID:
      op = RelationalExpression();
                                {if (true) return op;}
      break;
    default:
      jj_la1[7] = jj_gen;
      jj_consume_token(-1);
      throw new ParseException();
    }
    throw new Error("Missing return statement in function");
  }

  static final public AlgebraicOperator RelationalExpression() throws ParseException {
        Variable leftVar = null;
        Variable rightVar = null;
        String val = null;
        Qualification.Relationship qual = Qualification.Relationship.EQUALS;
        AlgebraicOperator op = null;
    leftVar = Attribute();
    qual = QualificationOperator();
    switch ((jj_ntk==-1)?jj_ntk():jj_ntk) {
    case ID:
      rightVar = Attribute();
                                VariableVariableQualification vvarq =
                                        new VariableVariableQualification(qual,
                                            leftVar, rightVar);
                                op = new Join(vvarq);
      break;
    case INTEGER_LITERAL:
    case FLOATING_POINT_LITERAL:
    case STRING_LITERAL:
      val = Literal();
                                VariableValueQualification vvalq =
                                        new VariableValueQualification(qual,
                                            leftVar, val);
                                op = new Selection(vvalq);
      break;
    default:
      jj_la1[8] = jj_gen;
      jj_consume_token(-1);
      throw new ParseException();
    }
                        {if (true) return op;}
    throw new Error("Missing return statement in function");
  }

  static final public void DisjunctionOperator() throws ParseException {
    jj_consume_token(OR);
  }

  static final public void ConjunctionOperator() throws ParseException {
    jj_consume_token(AND);
  }

  static final public void NegationOperator() throws ParseException {
    jj_consume_token(NOT);
  }

  static final public Qualification.Relationship QualificationOperator() throws ParseException {
    switch ((jj_ntk==-1)?jj_ntk():jj_ntk) {
    case LESS:
      jj_consume_token(LESS);
                                {if (true) return Qualification.Relationship.LESS;}
      break;
    case LESSEQUAL:
      jj_consume_token(LESSEQUAL);
                                {if (true) return Qualification.Relationship.LESS_EQUALS;}
      break;
    case GREATER:
      jj_consume_token(GREATER);
                                {if (true) return Qualification.Relationship.GREATER;}
      break;
    case GREATEREQUAL:
      jj_consume_token(GREATEREQUAL);
                                {if (true) return Qualification.Relationship.GREATER_EQUALS;}
      break;
    case EQUAL:
      jj_consume_token(EQUAL);
                                {if (true) return Qualification.Relationship.EQUALS;}
      break;
    case NOTEQUAL:
      jj_consume_token(NOTEQUAL);
                                {if (true) return Qualification.Relationship.NOT_EQUALS;}
      break;
    case NOTEQUAL2:
      jj_consume_token(NOTEQUAL2);
                                {if (true) return Qualification.Relationship.NOT_EQUALS;}
      break;
    default:
      jj_la1[9] = jj_gen;
      jj_consume_token(-1);
      throw new ParseException();
    }
    throw new Error("Missing return statement in function");
  }

  static final public void AliasedTable() throws ParseException {
    jj_consume_token(ID);
    jj_consume_token(ID);
  }

  static final public Variable QualifiedAttribute() throws ParseException {
        Token table = null;
        Token attr = null;
    table = jj_consume_token(ID);
    jj_consume_token(DOT);
    attr = jj_consume_token(ID);
                        {if (true) return new Variable(table.image, attr.image);}
    throw new Error("Missing return statement in function");
  }

  static final public String Identifier() throws ParseException {
        Token x = null;
    x = jj_consume_token(ID);
                        {if (true) return x.image;}
    throw new Error("Missing return statement in function");
  }

  static final public String Literal() throws ParseException {
        Token x = null;
    switch ((jj_ntk==-1)?jj_ntk():jj_ntk) {
    case STRING_LITERAL:
      x = jj_consume_token(STRING_LITERAL);
                                String s = x.image;
                                s = s.substring(1, s.length()-1);
                                {if (true) return s;}
      break;
    case INTEGER_LITERAL:
      x = jj_consume_token(INTEGER_LITERAL);
                                        {if (true) return x.image;}
      break;
    case FLOATING_POINT_LITERAL:
      x = jj_consume_token(FLOATING_POINT_LITERAL);
                                               {if (true) return x.image;}
      break;
    default:
      jj_la1[10] = jj_gen;
      jj_consume_token(-1);
      throw new ParseException();
    }
    throw new Error("Missing return statement in function");
  }

  static final public Table Create() throws ParseException {
        List<Attribute> v = null;
        String table;
    jj_consume_token(CREATE);
    jj_consume_token(TABLE);
    table = Identifier();
    jj_consume_token(OPENPAR);
    v = AttributeDeclarationList(table);
    jj_consume_token(CLOSEPAR);
                                {if (true) return new Table(table, v);}
    throw new Error("Missing return statement in function");
  }

  static final public List<Attribute> AttributeDeclarationList(String table) throws ParseException {
        List<Attribute> v = new ArrayList<Attribute>();
        TableAttribute tab = null;
    tab = AttributeDeclaration(table);
                        v.add(tab);
    label_5:
    while (true) {
      switch ((jj_ntk==-1)?jj_ntk():jj_ntk) {
      case COMMA:
        ;
        break;
      default:
        jj_la1[11] = jj_gen;
        break label_5;
      }
      jj_consume_token(COMMA);
      tab = AttributeDeclaration(table);
                                                v.add(tab);
    }
                        {if (true) return v;}
    throw new Error("Missing return statement in function");
  }

  static final public TableAttribute AttributeDeclaration(String table) throws ParseException {
        String name = null;
        Class<? extends Comparable> type = null;
    name = Identifier();
    type = Type();
                        {if (true) return new TableAttribute(table, name, type);}
    throw new Error("Missing return statement in function");
  }

  static final public Class<? extends Comparable> Type() throws ParseException {
        Token token = null;
    switch ((jj_ntk==-1)?jj_ntk():jj_ntk) {
    case INTEGER:
      jj_consume_token(INTEGER);
                                {if (true) return Integer.class;}
      break;
    case LONG:
      jj_consume_token(LONG);
                                {if (true) return Long.class;}
      break;
    case CHAR:
      jj_consume_token(CHAR);
                                {if (true) return Character.class;}
      break;
    case BYTE:
      jj_consume_token(BYTE);
                                {if (true) return Byte.class;}
      break;
    case SHORT:
      jj_consume_token(SHORT);
                                {if (true) return Short.class;}
      break;
    case DOUBLE:
      jj_consume_token(DOUBLE);
                                {if (true) return Double.class;}
      break;
    case FLOAT:
      jj_consume_token(FLOAT);
                                {if (true) return Float.class;}
      break;
    case STRING:
      jj_consume_token(STRING);
                                {if (true) return String.class;}
      break;
    default:
      jj_la1[12] = jj_gen;
      jj_consume_token(-1);
      throw new ParseException();
    }
    throw new Error("Missing return statement in function");
  }

  static final public Pair<String, List<Comparable>> Insert() throws ParseException {
        String table = null;
        List<Comparable> v = new ArrayList<Comparable>();
    jj_consume_token(INSERT);
    jj_consume_token(INTO);
    table = Identifier();
    jj_consume_token(VALUES);
    jj_consume_token(OPENPAR);
    v = ValueList();
    jj_consume_token(CLOSEPAR);
                        {if (true) return new Pair<String, List<Comparable>>(table, v);}
    throw new Error("Missing return statement in function");
  }

  static final public List<Comparable> ValueList() throws ParseException {
        List<Comparable> v = new ArrayList<Comparable>();
        String l = null;
    l = Literal();
                        v.add(l);
    label_6:
    while (true) {
      switch ((jj_ntk==-1)?jj_ntk():jj_ntk) {
      case COMMA:
        ;
        break;
      default:
        jj_la1[13] = jj_gen;
        break label_6;
      }
      jj_consume_token(COMMA);
      l = Literal();
                                v.add(l);
    }
                        {if (true) return v;}
    throw new Error("Missing return statement in function");
  }

  static final public String Drop() throws ParseException {
        String id = null;
    jj_consume_token(DROP);
    jj_consume_token(TABLE);
    id = Identifier();
                        {if (true) return id;}
    throw new Error("Missing return statement in function");
  }

  static final public void Catalog() throws ParseException {
    jj_consume_token(CATALOG);
  }

  static final public String Describe() throws ParseException {
        String s = null;
    jj_consume_token(DESCRIBE);
    jj_consume_token(TABLE);
    s = Identifier();
                        {if (true) return s;}
    throw new Error("Missing return statement in function");
  }

  static final private boolean jj_2_1(int xla) {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    try { return !jj_3_1(); }
    catch(LookaheadSuccess ls) { return true; }
    finally { jj_save(0, xla); }
  }

  static final private boolean jj_2_2(int xla) {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    try { return !jj_3_2(); }
    catch(LookaheadSuccess ls) { return true; }
    finally { jj_save(1, xla); }
  }

  static final private boolean jj_2_3(int xla) {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    try { return !jj_3_3(); }
    catch(LookaheadSuccess ls) { return true; }
    finally { jj_save(2, xla); }
  }

  static final private boolean jj_2_4(int xla) {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    try { return !jj_3_4(); }
    catch(LookaheadSuccess ls) { return true; }
    finally { jj_save(3, xla); }
  }

  static final private boolean jj_3R_7() {
    if (jj_scan_token(ID)) return true;
    if (jj_scan_token(ID)) return true;
    return false;
  }

  static final private boolean jj_3R_8() {
    if (jj_scan_token(ID)) return true;
    if (jj_scan_token(DOT)) return true;
    if (jj_scan_token(ID)) return true;
    return false;
  }

  static final private boolean jj_3_4() {
    if (jj_scan_token(12)) return true;
    return false;
  }

  static final private boolean jj_3_2() {
    if (jj_3R_8()) return true;
    return false;
  }

  static final private boolean jj_3_3() {
    if (jj_scan_token(13)) return true;
    return false;
  }

  static final private boolean jj_3_1() {
    if (jj_3R_7()) return true;
    return false;
  }

  static private boolean jj_initialized_once = false;
  static public SQLParserTokenManager token_source;
  static SimpleCharStream jj_input_stream;
  static public Token token, jj_nt;
  static private int jj_ntk;
  static private Token jj_scanpos, jj_lastpos;
  static private int jj_la;
  static public boolean lookingAhead = false;
  static private boolean jj_semLA;
  static private int jj_gen;
  static final private int[] jj_la1 = new int[14];
  static private int[] jj_la1_0;
  static private int[] jj_la1_1;
  static private int[] jj_la1_2;
  static {
      jj_la1_0();
      jj_la1_1();
      jj_la1_2();
   }
   private static void jj_la1_0() {
      jj_la1_0 = new int[] {0x8000,0x20000,0x40000,0x0,0x0,0x0,0x0,0x4000,0x580,0x0,0x580,0x0,0x0,0x0,};
   }
   private static void jj_la1_1() {
      jj_la1_1 = new int[] {0xe3,0x0,0x0,0x200,0x200,0x0,0x0,0x20000,0x0,0x1fc00,0x0,0x200,0xfe000000,0x200,};
   }
   private static void jj_la1_2() {
      jj_la1_2 = new int[] {0x0,0x0,0x0,0x0,0x0,0x2,0x2,0x2,0x2,0x0,0x0,0x0,0x1,0x0,};
   }
  static final private JJCalls[] jj_2_rtns = new JJCalls[4];
  static private boolean jj_rescan = false;
  static private int jj_gc = 0;

  public SQLParser(java.io.InputStream stream) {
     this(stream, null);
  }
  public SQLParser(java.io.InputStream stream, String encoding) {
    if (jj_initialized_once) {
      System.out.println("ERROR: Second call to constructor of static parser.  You must");
      System.out.println("       either use ReInit() or set the JavaCC option STATIC to false");
      System.out.println("       during parser generation.");
      throw new Error();
    }
    jj_initialized_once = true;
    try { jj_input_stream = new SimpleCharStream(stream, encoding, 1, 1); } catch(java.io.UnsupportedEncodingException e) { throw new RuntimeException(e); }
    token_source = new SQLParserTokenManager(jj_input_stream);
    token = new Token();
    jj_ntk = -1;
    jj_gen = 0;
    for (int i = 0; i < 14; i++) jj_la1[i] = -1;
    for (int i = 0; i < jj_2_rtns.length; i++) jj_2_rtns[i] = new JJCalls();
  }

  static public void ReInit(java.io.InputStream stream) {
     ReInit(stream, null);
  }
  static public void ReInit(java.io.InputStream stream, String encoding) {
    try { jj_input_stream.ReInit(stream, encoding, 1, 1); } catch(java.io.UnsupportedEncodingException e) { throw new RuntimeException(e); }
    token_source.ReInit(jj_input_stream);
    token = new Token();
    jj_ntk = -1;
    jj_gen = 0;
    for (int i = 0; i < 14; i++) jj_la1[i] = -1;
    for (int i = 0; i < jj_2_rtns.length; i++) jj_2_rtns[i] = new JJCalls();
  }

  public SQLParser(java.io.Reader stream) {
    if (jj_initialized_once) {
      System.out.println("ERROR: Second call to constructor of static parser.  You must");
      System.out.println("       either use ReInit() or set the JavaCC option STATIC to false");
      System.out.println("       during parser generation.");
      throw new Error();
    }
    jj_initialized_once = true;
    jj_input_stream = new SimpleCharStream(stream, 1, 1);
    token_source = new SQLParserTokenManager(jj_input_stream);
    token = new Token();
    jj_ntk = -1;
    jj_gen = 0;
    for (int i = 0; i < 14; i++) jj_la1[i] = -1;
    for (int i = 0; i < jj_2_rtns.length; i++) jj_2_rtns[i] = new JJCalls();
  }

  static public void ReInit(java.io.Reader stream) {
    jj_input_stream.ReInit(stream, 1, 1);
    token_source.ReInit(jj_input_stream);
    token = new Token();
    jj_ntk = -1;
    jj_gen = 0;
    for (int i = 0; i < 14; i++) jj_la1[i] = -1;
    for (int i = 0; i < jj_2_rtns.length; i++) jj_2_rtns[i] = new JJCalls();
  }

  public SQLParser(SQLParserTokenManager tm) {
    if (jj_initialized_once) {
      System.out.println("ERROR: Second call to constructor of static parser.  You must");
      System.out.println("       either use ReInit() or set the JavaCC option STATIC to false");
      System.out.println("       during parser generation.");
      throw new Error();
    }
    jj_initialized_once = true;
    token_source = tm;
    token = new Token();
    jj_ntk = -1;
    jj_gen = 0;
    for (int i = 0; i < 14; i++) jj_la1[i] = -1;
    for (int i = 0; i < jj_2_rtns.length; i++) jj_2_rtns[i] = new JJCalls();
  }

  public void ReInit(SQLParserTokenManager tm) {
    token_source = tm;
    token = new Token();
    jj_ntk = -1;
    jj_gen = 0;
    for (int i = 0; i < 14; i++) jj_la1[i] = -1;
    for (int i = 0; i < jj_2_rtns.length; i++) jj_2_rtns[i] = new JJCalls();
  }

  static final private Token jj_consume_token(int kind) throws ParseException {
    Token oldToken;
    if ((oldToken = token).next != null) token = token.next;
    else token = token.next = token_source.getNextToken();
    jj_ntk = -1;
    if (token.kind == kind) {
      jj_gen++;
      if (++jj_gc > 100) {
        jj_gc = 0;
        for (int i = 0; i < jj_2_rtns.length; i++) {
          JJCalls c = jj_2_rtns[i];
          while (c != null) {
            if (c.gen < jj_gen) c.first = null;
            c = c.next;
          }
        }
      }
      return token;
    }
    token = oldToken;
    jj_kind = kind;
    throw generateParseException();
  }

  static private final class LookaheadSuccess extends java.lang.Error { }
  static final private LookaheadSuccess jj_ls = new LookaheadSuccess();
  static final private boolean jj_scan_token(int kind) {
    if (jj_scanpos == jj_lastpos) {
      jj_la--;
      if (jj_scanpos.next == null) {
        jj_lastpos = jj_scanpos = jj_scanpos.next = token_source.getNextToken();
      } else {
        jj_lastpos = jj_scanpos = jj_scanpos.next;
      }
    } else {
      jj_scanpos = jj_scanpos.next;
    }
    if (jj_rescan) {
      int i = 0; Token tok = token;
      while (tok != null && tok != jj_scanpos) { i++; tok = tok.next; }
      if (tok != null) jj_add_error_token(kind, i);
    }
    if (jj_scanpos.kind != kind) return true;
    if (jj_la == 0 && jj_scanpos == jj_lastpos) throw jj_ls;
    return false;
  }

  static final public Token getNextToken() {
    if (token.next != null) token = token.next;
    else token = token.next = token_source.getNextToken();
    jj_ntk = -1;
    jj_gen++;
    return token;
  }

  static final public Token getToken(int index) {
    Token t = lookingAhead ? jj_scanpos : token;
    for (int i = 0; i < index; i++) {
      if (t.next != null) t = t.next;
      else t = t.next = token_source.getNextToken();
    }
    return t;
  }

  static final private int jj_ntk() {
    if ((jj_nt=token.next) == null)
      return (jj_ntk = (token.next=token_source.getNextToken()).kind);
    else
      return (jj_ntk = jj_nt.kind);
  }

  static private java.util.Vector<int[]> jj_expentries = new java.util.Vector<int[]>();
  static private int[] jj_expentry;
  static private int jj_kind = -1;
  static private int[] jj_lasttokens = new int[100];
  static private int jj_endpos;

  static private void jj_add_error_token(int kind, int pos) {
    if (pos >= 100) return;
    if (pos == jj_endpos + 1) {
      jj_lasttokens[jj_endpos++] = kind;
    } else if (jj_endpos != 0) {
      jj_expentry = new int[jj_endpos];
      for (int i = 0; i < jj_endpos; i++) {
        jj_expentry[i] = jj_lasttokens[i];
      }
      boolean exists = false;
      for (java.util.Enumeration e = jj_expentries.elements(); e.hasMoreElements();) {
        int[] oldentry = (int[])(e.nextElement());
        if (oldentry.length == jj_expentry.length) {
          exists = true;
          for (int i = 0; i < jj_expentry.length; i++) {
            if (oldentry[i] != jj_expentry[i]) {
              exists = false;
              break;
            }
          }
          if (exists) break;
        }
      }
      if (!exists) jj_expentries.addElement(jj_expentry);
      if (pos != 0) jj_lasttokens[(jj_endpos = pos) - 1] = kind;
    }
  }

  static public ParseException generateParseException() {
    jj_expentries.removeAllElements();
    boolean[] la1tokens = new boolean[68];
    for (int i = 0; i < 68; i++) {
      la1tokens[i] = false;
    }
    if (jj_kind >= 0) {
      la1tokens[jj_kind] = true;
      jj_kind = -1;
    }
    for (int i = 0; i < 14; i++) {
      if (jj_la1[i] == jj_gen) {
        for (int j = 0; j < 32; j++) {
          if ((jj_la1_0[i] & (1<<j)) != 0) {
            la1tokens[j] = true;
          }
          if ((jj_la1_1[i] & (1<<j)) != 0) {
            la1tokens[32+j] = true;
          }
          if ((jj_la1_2[i] & (1<<j)) != 0) {
            la1tokens[64+j] = true;
          }
        }
      }
    }
    for (int i = 0; i < 68; i++) {
      if (la1tokens[i]) {
        jj_expentry = new int[1];
        jj_expentry[0] = i;
        jj_expentries.addElement(jj_expentry);
      }
    }
    jj_endpos = 0;
    jj_rescan_token();
    jj_add_error_token(0, 0);
    int[][] exptokseq = new int[jj_expentries.size()][];
    for (int i = 0; i < jj_expentries.size(); i++) {
      exptokseq[i] = (int[])jj_expentries.elementAt(i);
    }
    return new ParseException(token, exptokseq, tokenImage);
  }

  static final public void enable_tracing() {
  }

  static final public void disable_tracing() {
  }

  static final private void jj_rescan_token() {
    jj_rescan = true;
    for (int i = 0; i < 4; i++) {
    try {
      JJCalls p = jj_2_rtns[i];
      do {
        if (p.gen > jj_gen) {
          jj_la = p.arg; jj_lastpos = jj_scanpos = p.first;
          switch (i) {
            case 0: jj_3_1(); break;
            case 1: jj_3_2(); break;
            case 2: jj_3_3(); break;
            case 3: jj_3_4(); break;
          }
        }
        p = p.next;
      } while (p != null);
      } catch(LookaheadSuccess ls) { }
    }
    jj_rescan = false;
  }

  static final private void jj_save(int index, int xla) {
    JJCalls p = jj_2_rtns[index];
    while (p.gen > jj_gen) {
      if (p.next == null) { p = p.next = new JJCalls(); break; }
      p = p.next;
    }
    p.gen = jj_gen + xla - jj_la; p.first = token; p.arg = xla;
  }

  static final class JJCalls {
    int gen;
    Token first;
    int arg;
    JJCalls next;
  }

}
/* Generated By:JavaCC: Do not edit this line. SQLParserConstants.java */

interface SQLParserConstants {

  int EOF = 0;
  int COMMENT_LINE = 5;
  int COMMENT_BLOCK = 6;
  int INTEGER_LITERAL = 7;
  int FLOATING_POINT_LITERAL = 8;
  int EXPONENT = 9;
  int STRING_LITERAL = 10;
  int ALL = 11;
  int AND = 12;
  int OR = 13;
  int NOT = 14;
  int SELECT = 15;
  int FROM = 16;
  int WHERE = 17;
  int ORDER = 18;
  int GROUP = 19;
  int MAX = 20;
  int MIN = 21;
  int HAVING = 22;
  int DISTINCT = 23;
  int IN = 24;
  int EXISTS = 25;
  int COUNT = 26;
  int ASC = 27;
  int DESC = 28;
  int SUM = 29;
  int BY = 30;
  int BETWEEN = 31;
  int CREATE = 32;
  int INSERT = 33;
  int TABLE = 34;
  int VALUES = 35;
  int INTO = 36;
  int DROP = 37;
  int DESCRIBE = 38;
  int CATALOG = 39;
  int DOT = 40;
  int COMMA = 41;
  int LESS = 42;
  int LESSEQUAL = 43;
  int GREATER = 44;
  int GREATEREQUAL = 45;
  int EQUAL = 46;
  int NOTEQUAL = 47;
  int NOTEQUAL2 = 48;
  int OPENPAR = 49;
  int CLOSEPAR = 50;
  int ASTERISK = 51;
  int SLASH = 52;
  int PLUS = 53;
  int MINUS = 54;
  int QUESTIONMARK = 55;
  int PERCENT = 56;
  int INTEGER = 57;
  int LONG = 58;
  int CHAR = 59;
  int BYTE = 60;
  int SHORT = 61;
  int DOUBLE = 62;
  int FLOAT = 63;
  int STRING = 64;
  int ID = 65;
  int LETTER = 66;
  int DIGIT = 67;

  int DEFAULT = 0;

  String[] tokenImage = {
    "<EOF>",
    "\" \"",
    "\"\\t\"",
    "\"\\n\"",
    "\"\\r\"",
    "<COMMENT_LINE>",
    "<COMMENT_BLOCK>",
    "<INTEGER_LITERAL>",
    "<FLOATING_POINT_LITERAL>",
    "<EXPONENT>",
    "<STRING_LITERAL>",
    "\"all\"",
    "\"and\"",
    "\"or\"",
    "\"not\"",
    "\"select\"",
    "\"from\"",
    "\"where\"",
    "\"order\"",
    "\"group\"",
    "\"max\"",
    "\"min\"",
    "\"having\"",
    "\"distinct\"",
    "\"in\"",
    "\"exists\"",
    "\"count\"",
    "\"asc\"",
    "\"desc\"",
    "\"sum\"",
    "\"by\"",
    "\"between\"",
    "\"create\"",
    "\"insert\"",
    "\"table\"",
    "\"values\"",
    "\"into\"",
    "\"drop\"",
    "\"describe\"",
    "\"catalog\"",
    "\".\"",
    "\",\"",
    "\"<\"",
    "\"<=\"",
    "\">\"",
    "\">=\"",
    "\"=\"",
    "\"!=\"",
    "\"<>\"",
    "\"(\"",
    "\")\"",
    "\"*\"",
    "\"/\"",
    "\"+\"",
    "\"-\"",
    "\"?\"",
    "\"%\"",
    "\"integer\"",
    "\"long\"",
    "\"character\"",
    "\"byte\"",
    "\"short\"",
    "\"double\"",
    "\"float\"",
    "\"string\"",
    "<ID>",
    "<LETTER>",
    "<DIGIT>",
  };

}
/* Generated By:JavaCC: Do not edit this line. SQLParserTokenManager.java */

class SQLParserTokenManager implements SQLParserConstants
{
  public static  java.io.PrintStream debugStream = System.out;
  public static  void setDebugStream(java.io.PrintStream ds) { debugStream = ds; }
private static final int jjStopStringLiteralDfa_0(int pos, long active0, long active1)
{
   switch (pos)
   {
      case 0:
         if ((active0 & 0x40000000000000L) != 0L)
            return 0;
         if ((active0 & 0xfe0000fffffff800L) != 0L || (active1 & 0x1L) != 0L)
         {
            jjmatchedKind = 65;
            return 42;
         }
         if ((active0 & 0x10000000000L) != 0L)
            return 14;
         if ((active0 & 0x10000000000000L) != 0L)
            return 6;
         return -1;
      case 1:
         if ((active0 & 0x1200001241042000L) != 0L)
            return 42;
         if ((active0 & 0xec0000edbefbd800L) != 0L || (active1 & 0x1L) != 0L)
         {
            if (jjmatchedPos != 1)
            {
               jjmatchedKind = 65;
               jjmatchedPos = 1;
            }
            return 42;
         }
         return -1;
      case 2:
         if ((active0 & 0x28305800L) != 0L)
            return 42;
         if ((active0 & 0xfe0000ff96cf8000L) != 0L || (active1 & 0x1L) != 0L)
         {
            jjmatchedKind = 65;
            jjmatchedPos = 2;
            return 42;
         }
         return -1;
      case 3:
         if ((active0 & 0xea00008f86ce8000L) != 0L || (active1 & 0x1L) != 0L)
         {
            if (jjmatchedPos != 3)
            {
               jjmatchedKind = 65;
               jjmatchedPos = 3;
            }
            return 42;
         }
         if ((active0 & 0x1400007010010000L) != 0L)
            return 42;
         return -1;
      case 4:
         if ((active0 & 0xa0000004040e0000L) != 0L)
            return 42;
         if ((active0 & 0x4a0000cb82c08000L) != 0L || (active1 & 0x1L) != 0L)
         {
            jjmatchedKind = 65;
            jjmatchedPos = 4;
            return 42;
         }
         return -1;
      case 5:
         if ((active0 & 0x4000000b02408000L) != 0L || (active1 & 0x1L) != 0L)
            return 42;
         if ((active0 & 0xa0000c080800000L) != 0L)
         {
            jjmatchedKind = 65;
            jjmatchedPos = 5;
            return 42;
         }
         return -1;
      case 6:
         if ((active0 & 0x800004000800000L) != 0L)
         {
            jjmatchedKind = 65;
            jjmatchedPos = 6;
            return 42;
         }
         if ((active0 & 0x200008080000000L) != 0L)
            return 42;
         return -1;
      case 7:
         if ((active0 & 0x4000800000L) != 0L)
            return 42;
         if ((active0 & 0x800000000000000L) != 0L)
         {
            jjmatchedKind = 65;
            jjmatchedPos = 7;
            return 42;
         }
         return -1;
      default :
         return -1;
   }
}
private static final int jjStartNfa_0(int pos, long active0, long active1)
{
   return jjMoveNfa_0(jjStopStringLiteralDfa_0(pos, active0, active1), pos + 1);
}
static private final int jjStopAtPos(int pos, int kind)
{
   jjmatchedKind = kind;
   jjmatchedPos = pos;
   return pos + 1;
}
static private final int jjStartNfaWithStates_0(int pos, int kind, int state)
{
   jjmatchedKind = kind;
   jjmatchedPos = pos;
   try { curChar = input_stream.readChar(); }
   catch(java.io.IOException e) { return pos + 1; }
   return jjMoveNfa_0(state, pos + 1);
}
static private final int jjMoveStringLiteralDfa0_0()
{
   switch(curChar)
   {
      case 33:
         return jjMoveStringLiteralDfa1_0(0x800000000000L, 0x0L);
      case 37:
         return jjStopAtPos(0, 56);
      case 40:
         return jjStopAtPos(0, 49);
      case 41:
         return jjStopAtPos(0, 50);
      case 42:
         return jjStopAtPos(0, 51);
      case 43:
         return jjStopAtPos(0, 53);
      case 44:
         return jjStopAtPos(0, 41);
      case 45:
         return jjStartNfaWithStates_0(0, 54, 0);
      case 46:
         return jjStartNfaWithStates_0(0, 40, 14);
      case 47:
         return jjStartNfaWithStates_0(0, 52, 6);
      case 60:
         jjmatchedKind = 42;
         return jjMoveStringLiteralDfa1_0(0x1080000000000L, 0x0L);
      case 61:
         return jjStopAtPos(0, 46);
      case 62:
         jjmatchedKind = 44;
         return jjMoveStringLiteralDfa1_0(0x200000000000L, 0x0L);
      case 63:
         return jjStopAtPos(0, 55);
      case 97:
         return jjMoveStringLiteralDfa1_0(0x8001800L, 0x0L);
      case 98:
         return jjMoveStringLiteralDfa1_0(0x10000000c0000000L, 0x0L);
      case 99:
         return jjMoveStringLiteralDfa1_0(0x800008104000000L, 0x0L);
      case 100:
         return jjMoveStringLiteralDfa1_0(0x4000006010800000L, 0x0L);
      case 101:
         return jjMoveStringLiteralDfa1_0(0x2000000L, 0x0L);
      case 102:
         return jjMoveStringLiteralDfa1_0(0x8000000000010000L, 0x0L);
      case 103:
         return jjMoveStringLiteralDfa1_0(0x80000L, 0x0L);
      case 104:
         return jjMoveStringLiteralDfa1_0(0x400000L, 0x0L);
      case 105:
         return jjMoveStringLiteralDfa1_0(0x200001201000000L, 0x0L);
      case 108:
         return jjMoveStringLiteralDfa1_0(0x400000000000000L, 0x0L);
      case 109:
         return jjMoveStringLiteralDfa1_0(0x300000L, 0x0L);
      case 110:
         return jjMoveStringLiteralDfa1_0(0x4000L, 0x0L);
      case 111:
         return jjMoveStringLiteralDfa1_0(0x42000L, 0x0L);
      case 115:
         return jjMoveStringLiteralDfa1_0(0x2000000020008000L, 0x1L);
      case 116:
         return jjMoveStringLiteralDfa1_0(0x400000000L, 0x0L);
      case 118:
         return jjMoveStringLiteralDfa1_0(0x800000000L, 0x0L);
      case 119:
         return jjMoveStringLiteralDfa1_0(0x20000L, 0x0L);
      default :
         return jjMoveNfa_0(5, 0);
   }
}
static private final int jjMoveStringLiteralDfa1_0(long active0, long active1)
{
   try { curChar = input_stream.readChar(); }
   catch(java.io.IOException e) {
      jjStopStringLiteralDfa_0(0, active0, active1);
      return 1;
   }
   switch(curChar)
   {
      case 61:
         if ((active0 & 0x80000000000L) != 0L)
            return jjStopAtPos(1, 43);
         else if ((active0 & 0x200000000000L) != 0L)
            return jjStopAtPos(1, 45);
         else if ((active0 & 0x800000000000L) != 0L)
            return jjStopAtPos(1, 47);
         break;
      case 62:
         if ((active0 & 0x1000000000000L) != 0L)
            return jjStopAtPos(1, 48);
         break;
      case 97:
         return jjMoveStringLiteralDfa2_0(active0, 0x8c00500000L, active1, 0L);
      case 101:
         return jjMoveStringLiteralDfa2_0(active0, 0x4090008000L, active1, 0L);
      case 104:
         return jjMoveStringLiteralDfa2_0(active0, 0x2800000000020000L, active1, 0L);
      case 105:
         return jjMoveStringLiteralDfa2_0(active0, 0xa00000L, active1, 0L);
      case 108:
         return jjMoveStringLiteralDfa2_0(active0, 0x8000000000000800L, active1, 0L);
      case 110:
         if ((active0 & 0x1000000L) != 0L)
         {
            jjmatchedKind = 24;
            jjmatchedPos = 1;
         }
         return jjMoveStringLiteralDfa2_0(active0, 0x200001200001000L, active1, 0L);
      case 111:
         return jjMoveStringLiteralDfa2_0(active0, 0x4400000004004000L, active1, 0L);
      case 114:
         if ((active0 & 0x2000L) != 0L)
         {
            jjmatchedKind = 13;
            jjmatchedPos = 1;
         }
         return jjMoveStringLiteralDfa2_0(active0, 0x21000d0000L, active1, 0L);
      case 115:
         return jjMoveStringLiteralDfa2_0(active0, 0x8000000L, active1, 0L);
      case 116:
         return jjMoveStringLiteralDfa2_0(active0, 0L, active1, 0x1L);
      case 117:
         return jjMoveStringLiteralDfa2_0(active0, 0x20000000L, active1, 0L);
      case 120:
         return jjMoveStringLiteralDfa2_0(active0, 0x2000000L, active1, 0L);
      case 121:
         if ((active0 & 0x40000000L) != 0L)
         {
            jjmatchedKind = 30;
            jjmatchedPos = 1;
         }
         return jjMoveStringLiteralDfa2_0(active0, 0x1000000000000000L, active1, 0L);
      default :
         break;
   }
   return jjStartNfa_0(0, active0, active1);
}
static private final int jjMoveStringLiteralDfa2_0(long old0, long active0, long old1, long active1)
{
   if (((active0 &= old0) | (active1 &= old1)) == 0L)
      return jjStartNfa_0(0, old0, old1); 
   try { curChar = input_stream.readChar(); }
   catch(java.io.IOException e) {
      jjStopStringLiteralDfa_0(1, active0, active1);
      return 2;
   }
   switch(curChar)
   {
      case 97:
         return jjMoveStringLiteralDfa3_0(active0, 0x800000000000000L, active1, 0L);
      case 98:
         return jjMoveStringLiteralDfa3_0(active0, 0x400000000L, active1, 0L);
      case 99:
         if ((active0 & 0x8000000L) != 0L)
            return jjStartNfaWithStates_0(2, 27, 42);
         break;
      case 100:
         if ((active0 & 0x1000L) != 0L)
            return jjStartNfaWithStates_0(2, 12, 42);
         return jjMoveStringLiteralDfa3_0(active0, 0x40000L, active1, 0L);
      case 101:
         return jjMoveStringLiteralDfa3_0(active0, 0x100020000L, active1, 0L);
      case 105:
         return jjMoveStringLiteralDfa3_0(active0, 0x2000000L, active1, 0L);
      case 108:
         if ((active0 & 0x800L) != 0L)
            return jjStartNfaWithStates_0(2, 11, 42);
         return jjMoveStringLiteralDfa3_0(active0, 0x800008000L, active1, 0L);
      case 109:
         if ((active0 & 0x20000000L) != 0L)
            return jjStartNfaWithStates_0(2, 29, 42);
         break;
      case 110:
         if ((active0 & 0x200000L) != 0L)
            return jjStartNfaWithStates_0(2, 21, 42);
         return jjMoveStringLiteralDfa3_0(active0, 0x400000000000000L, active1, 0L);
      case 111:
         return jjMoveStringLiteralDfa3_0(active0, 0xa000002000090000L, active1, 0L);
      case 114:
         return jjMoveStringLiteralDfa3_0(active0, 0L, active1, 0x1L);
      case 115:
         return jjMoveStringLiteralDfa3_0(active0, 0x4210800000L, active1, 0L);
      case 116:
         if ((active0 & 0x4000L) != 0L)
            return jjStartNfaWithStates_0(2, 14, 42);
         return jjMoveStringLiteralDfa3_0(active0, 0x1200009080000000L, active1, 0L);
      case 117:
         return jjMoveStringLiteralDfa3_0(active0, 0x4000000004000000L, active1, 0L);
      case 118:
         return jjMoveStringLiteralDfa3_0(active0, 0x400000L, active1, 0L);
      case 120:
         if ((active0 & 0x100000L) != 0L)
            return jjStartNfaWithStates_0(2, 20, 42);
         break;
      default :
         break;
   }
   return jjStartNfa_0(1, active0, active1);
}
static private final int jjMoveStringLiteralDfa3_0(long old0, long active0, long old1, long active1)
{
   if (((active0 &= old0) | (active1 &= old1)) == 0L)
      return jjStartNfa_0(1, old0, old1); 
   try { curChar = input_stream.readChar(); }
   catch(java.io.IOException e) {
      jjStopStringLiteralDfa_0(2, active0, active1);
      return 3;
   }
   switch(curChar)
   {
      case 97:
         return jjMoveStringLiteralDfa4_0(active0, 0x8000008100000000L, active1, 0L);
      case 98:
         return jjMoveStringLiteralDfa4_0(active0, 0x4000000000000000L, active1, 0L);
      case 99:
         if ((active0 & 0x10000000L) != 0L)
         {
            jjmatchedKind = 28;
            jjmatchedPos = 3;
         }
         return jjMoveStringLiteralDfa4_0(active0, 0x4000000000L, active1, 0L);
      case 101:
         if ((active0 & 0x1000000000000000L) != 0L)
            return jjStartNfaWithStates_0(3, 60, 42);
         return jjMoveStringLiteralDfa4_0(active0, 0x200000200048000L, active1, 0L);
      case 103:
         if ((active0 & 0x400000000000000L) != 0L)
            return jjStartNfaWithStates_0(3, 58, 42);
         break;
      case 105:
         return jjMoveStringLiteralDfa4_0(active0, 0x400000L, active1, 0x1L);
      case 108:
         return jjMoveStringLiteralDfa4_0(active0, 0x400000000L, active1, 0L);
      case 109:
         if ((active0 & 0x10000L) != 0L)
            return jjStartNfaWithStates_0(3, 16, 42);
         break;
      case 110:
         return jjMoveStringLiteralDfa4_0(active0, 0x4000000L, active1, 0L);
      case 111:
         if ((active0 & 0x1000000000L) != 0L)
            return jjStartNfaWithStates_0(3, 36, 42);
         break;
      case 112:
         if ((active0 & 0x2000000000L) != 0L)
            return jjStartNfaWithStates_0(3, 37, 42);
         break;
      case 114:
         return jjMoveStringLiteralDfa4_0(active0, 0x2800000000020000L, active1, 0L);
      case 115:
         return jjMoveStringLiteralDfa4_0(active0, 0x2000000L, active1, 0L);
      case 116:
         return jjMoveStringLiteralDfa4_0(active0, 0x800000L, active1, 0L);
      case 117:
         return jjMoveStringLiteralDfa4_0(active0, 0x800080000L, active1, 0L);
      case 119:
         return jjMoveStringLiteralDfa4_0(active0, 0x80000000L, active1, 0L);
      default :
         break;
   }
   return jjStartNfa_0(2, active0, active1);
}
static private final int jjMoveStringLiteralDfa4_0(long old0, long active0, long old1, long active1)
{
   if (((active0 &= old0) | (active1 &= old1)) == 0L)
      return jjStartNfa_0(2, old0, old1); 
   try { curChar = input_stream.readChar(); }
   catch(java.io.IOException e) {
      jjStopStringLiteralDfa_0(3, active0, active1);
      return 4;
   }
   switch(curChar)
   {
      case 97:
         return jjMoveStringLiteralDfa5_0(active0, 0x800000000000000L, active1, 0L);
      case 99:
         return jjMoveStringLiteralDfa5_0(active0, 0x8000L, active1, 0L);
      case 101:
         if ((active0 & 0x20000L) != 0L)
            return jjStartNfaWithStates_0(4, 17, 42);
         else if ((active0 & 0x400000000L) != 0L)
            return jjStartNfaWithStates_0(4, 34, 42);
         return jjMoveStringLiteralDfa5_0(active0, 0x880000000L, active1, 0L);
      case 103:
         return jjMoveStringLiteralDfa5_0(active0, 0x200000000000000L, active1, 0L);
      case 105:
         return jjMoveStringLiteralDfa5_0(active0, 0x800000L, active1, 0L);
      case 108:
         return jjMoveStringLiteralDfa5_0(active0, 0x4000008000000000L, active1, 0L);
      case 110:
         return jjMoveStringLiteralDfa5_0(active0, 0x400000L, active1, 0x1L);
      case 112:
         if ((active0 & 0x80000L) != 0L)
            return jjStartNfaWithStates_0(4, 19, 42);
         break;
      case 114:
         if ((active0 & 0x40000L) != 0L)
            return jjStartNfaWithStates_0(4, 18, 42);
         return jjMoveStringLiteralDfa5_0(active0, 0x4200000000L, active1, 0L);
      case 116:
         if ((active0 & 0x4000000L) != 0L)
            return jjStartNfaWithStates_0(4, 26, 42);
         else if ((active0 & 0x2000000000000000L) != 0L)
            return jjStartNfaWithStates_0(4, 61, 42);
         else if ((active0 & 0x8000000000000000L) != 0L)
            return jjStartNfaWithStates_0(4, 63, 42);
         return jjMoveStringLiteralDfa5_0(active0, 0x102000000L, active1, 0L);
      default :
         break;
   }
   return jjStartNfa_0(3, active0, active1);
}
static private final int jjMoveStringLiteralDfa5_0(long old0, long active0, long old1, long active1)
{
   if (((active0 &= old0) | (active1 &= old1)) == 0L)
      return jjStartNfa_0(3, old0, old1); 
   try { curChar = input_stream.readChar(); }
   catch(java.io.IOException e) {
      jjStopStringLiteralDfa_0(4, active0, active1);
      return 5;
   }
   switch(curChar)
   {
      case 99:
         return jjMoveStringLiteralDfa6_0(active0, 0x800000000000000L, active1, 0L);
      case 101:
         if ((active0 & 0x100000000L) != 0L)
            return jjStartNfaWithStates_0(5, 32, 42);
         else if ((active0 & 0x4000000000000000L) != 0L)
            return jjStartNfaWithStates_0(5, 62, 42);
         return jjMoveStringLiteralDfa6_0(active0, 0x200000080000000L, active1, 0L);
      case 103:
         if ((active0 & 0x400000L) != 0L)
            return jjStartNfaWithStates_0(5, 22, 42);
         else if ((active1 & 0x1L) != 0L)
            return jjStartNfaWithStates_0(5, 64, 42);
         break;
      case 105:
         return jjMoveStringLiteralDfa6_0(active0, 0x4000000000L, active1, 0L);
      case 110:
         return jjMoveStringLiteralDfa6_0(active0, 0x800000L, active1, 0L);
      case 111:
         return jjMoveStringLiteralDfa6_0(active0, 0x8000000000L, active1, 0L);
      case 115:
         if ((active0 & 0x2000000L) != 0L)
            return jjStartNfaWithStates_0(5, 25, 42);
         else if ((active0 & 0x800000000L) != 0L)
            return jjStartNfaWithStates_0(5, 35, 42);
         break;
      case 116:
         if ((active0 & 0x8000L) != 0L)
            return jjStartNfaWithStates_0(5, 15, 42);
         else if ((active0 & 0x200000000L) != 0L)
            return jjStartNfaWithStates_0(5, 33, 42);
         break;
      default :
         break;
   }
   return jjStartNfa_0(4, active0, active1);
}
static private final int jjMoveStringLiteralDfa6_0(long old0, long active0, long old1, long active1)
{
   if (((active0 &= old0) | (active1 &= old1)) == 0L)
      return jjStartNfa_0(4, old0, old1); 
   try { curChar = input_stream.readChar(); }
   catch(java.io.IOException e) {
      jjStopStringLiteralDfa_0(5, active0, 0L);
      return 6;
   }
   switch(curChar)
   {
      case 98:
         return jjMoveStringLiteralDfa7_0(active0, 0x4000000000L);
      case 99:
         return jjMoveStringLiteralDfa7_0(active0, 0x800000L);
      case 103:
         if ((active0 & 0x8000000000L) != 0L)
            return jjStartNfaWithStates_0(6, 39, 42);
         break;
      case 110:
         if ((active0 & 0x80000000L) != 0L)
            return jjStartNfaWithStates_0(6, 31, 42);
         break;
      case 114:
         if ((active0 & 0x200000000000000L) != 0L)
            return jjStartNfaWithStates_0(6, 57, 42);
         break;
      case 116:
         return jjMoveStringLiteralDfa7_0(active0, 0x800000000000000L);
      default :
         break;
   }
   return jjStartNfa_0(5, active0, 0L);
}
static private final int jjMoveStringLiteralDfa7_0(long old0, long active0)
{
   if (((active0 &= old0)) == 0L)
      return jjStartNfa_0(5, old0, 0L);
   try { curChar = input_stream.readChar(); }
   catch(java.io.IOException e) {
      jjStopStringLiteralDfa_0(6, active0, 0L);
      return 7;
   }
   switch(curChar)
   {
      case 101:
         if ((active0 & 0x4000000000L) != 0L)
            return jjStartNfaWithStates_0(7, 38, 42);
         return jjMoveStringLiteralDfa8_0(active0, 0x800000000000000L);
      case 116:
         if ((active0 & 0x800000L) != 0L)
            return jjStartNfaWithStates_0(7, 23, 42);
         break;
      default :
         break;
   }
   return jjStartNfa_0(6, active0, 0L);
}
static private final int jjMoveStringLiteralDfa8_0(long old0, long active0)
{
   if (((active0 &= old0)) == 0L)
      return jjStartNfa_0(6, old0, 0L);
   try { curChar = input_stream.readChar(); }
   catch(java.io.IOException e) {
      jjStopStringLiteralDfa_0(7, active0, 0L);
      return 8;
   }
   switch(curChar)
   {
      case 114:
         if ((active0 & 0x800000000000000L) != 0L)
            return jjStartNfaWithStates_0(8, 59, 42);
         break;
      default :
         break;
   }
   return jjStartNfa_0(7, active0, 0L);
}
static private final void jjCheckNAdd(int state)
{
   if (jjrounds[state] != jjround)
   {
      jjstateSet[jjnewStateCnt++] = state;
      jjrounds[state] = jjround;
   }
}
static private final void jjAddStates(int start, int end)
{
   do {
      jjstateSet[jjnewStateCnt++] = jjnextStates[start];
   } while (start++ != end);
}
static private final void jjCheckNAddTwoStates(int state1, int state2)
{
   jjCheckNAdd(state1);
   jjCheckNAdd(state2);
}
static private final void jjCheckNAddStates(int start, int end)
{
   do {
      jjCheckNAdd(jjnextStates[start]);
   } while (start++ != end);
}
static private final void jjCheckNAddStates(int start)
{
   jjCheckNAdd(jjnextStates[start]);
   jjCheckNAdd(jjnextStates[start + 1]);
}
static final long[] jjbitVec0 = {
   0x0L, 0x0L, 0xffffffffffffffffL, 0xffffffffffffffffL
};
static private final int jjMoveNfa_0(int startState, int curPos)
{
   int[] nextStates;
   int startsAt = 0;
   jjnewStateCnt = 42;
   int i = 1;
   jjstateSet[0] = startState;
   int j, kind = 0x7fffffff;
   for (;;)
   {
      if (++jjround == 0x7fffffff)
         ReInitRounds();
      if (curChar < 64)
      {
         long l = 1L << curChar;
         MatchLoop: do
         {
            switch(jjstateSet[--i])
            {
               case 5:
                  if ((0x3ff000000000000L & l) != 0L)
                  {
                     if (kind > 7)
                        kind = 7;
                     jjCheckNAddStates(0, 6);
                  }
                  else if (curChar == 39)
                     jjCheckNAddStates(7, 9);
                  else if (curChar == 46)
                     jjCheckNAdd(14);
                  else if (curChar == 47)
                     jjstateSet[jjnewStateCnt++] = 6;
                  else if (curChar == 45)
                     jjstateSet[jjnewStateCnt++] = 0;
                  break;
               case 42:
               case 25:
                  if ((0x3ff001800000000L & l) == 0L)
                     break;
                  if (kind > 65)
                     kind = 65;
                  jjCheckNAdd(25);
                  break;
               case 0:
                  if (curChar == 45)
                     jjCheckNAddStates(10, 12);
                  break;
               case 1:
                  if ((0xffffffffffffdbffL & l) != 0L)
                     jjCheckNAddStates(10, 12);
                  break;
               case 2:
                  if ((0x2400L & l) != 0L && kind > 5)
                     kind = 5;
                  break;
               case 3:
                  if (curChar == 10 && kind > 5)
                     kind = 5;
                  break;
               case 4:
                  if (curChar == 13)
                     jjstateSet[jjnewStateCnt++] = 3;
                  break;
               case 6:
                  if (curChar == 42)
                     jjCheckNAddTwoStates(7, 8);
                  break;
               case 7:
                  if ((0xfffffbffffffffffL & l) != 0L)
                     jjCheckNAddTwoStates(7, 8);
                  break;
               case 8:
                  if (curChar == 42)
                     jjCheckNAddStates(13, 15);
                  break;
               case 9:
                  if ((0xffff7bffffffffffL & l) != 0L)
                     jjCheckNAddTwoStates(10, 8);
                  break;
               case 10:
                  if ((0xfffffbffffffffffL & l) != 0L)
                     jjCheckNAddTwoStates(10, 8);
                  break;
               case 11:
                  if (curChar == 47 && kind > 6)
                     kind = 6;
                  break;
               case 12:
                  if (curChar == 47)
                     jjstateSet[jjnewStateCnt++] = 6;
                  break;
               case 13:
                  if (curChar == 46)
                     jjCheckNAdd(14);
                  break;
               case 14:
                  if ((0x3ff000000000000L & l) == 0L)
                     break;
                  if (kind > 8)
                     kind = 8;
                  jjCheckNAddTwoStates(14, 15);
                  break;
               case 16:
                  if ((0x280000000000L & l) != 0L)
                     jjCheckNAdd(17);
                  break;
               case 17:
                  if ((0x3ff000000000000L & l) == 0L)
                     break;
                  if (kind > 8)
                     kind = 8;
                  jjCheckNAdd(17);
                  break;
               case 18:
                  if (curChar == 39)
                     jjCheckNAddStates(7, 9);
                  break;
               case 19:
                  if ((0xffffff7fffffffffL & l) != 0L)
                     jjCheckNAddStates(7, 9);
                  break;
               case 20:
                  if (curChar == 39)
                     jjCheckNAddStates(16, 18);
                  break;
               case 21:
                  if (curChar == 39)
                     jjstateSet[jjnewStateCnt++] = 20;
                  break;
               case 22:
                  if ((0xffffff7fffffffffL & l) != 0L)
                     jjCheckNAddStates(16, 18);
                  break;
               case 23:
                  if (curChar == 39 && kind > 10)
                     kind = 10;
                  break;
               case 26:
                  if ((0x3ff000000000000L & l) == 0L)
                     break;
                  if (kind > 7)
                     kind = 7;
                  jjCheckNAddStates(0, 6);
                  break;
               case 27:
                  if ((0x3ff000000000000L & l) == 0L)
                     break;
                  if (kind > 7)
                     kind = 7;
                  jjCheckNAdd(27);
                  break;
               case 28:
                  if ((0x3ff000000000000L & l) != 0L)
                     jjCheckNAddTwoStates(28, 29);
                  break;
               case 29:
                  if (curChar == 46)
                     jjCheckNAdd(30);
                  break;
               case 30:
                  if ((0x3ff000000000000L & l) == 0L)
                     break;
                  if (kind > 8)
                     kind = 8;
                  jjCheckNAddTwoStates(30, 31);
                  break;
               case 32:
                  if ((0x280000000000L & l) != 0L)
                     jjCheckNAdd(33);
                  break;
               case 33:
                  if ((0x3ff000000000000L & l) == 0L)
                     break;
                  if (kind > 8)
                     kind = 8;
                  jjCheckNAdd(33);
                  break;
               case 34:
                  if ((0x3ff000000000000L & l) != 0L)
                     jjCheckNAddTwoStates(34, 35);
                  break;
               case 36:
                  if ((0x280000000000L & l) != 0L)
                     jjCheckNAdd(37);
                  break;
               case 37:
                  if ((0x3ff000000000000L & l) == 0L)
                     break;
                  if (kind > 8)
                     kind = 8;
                  jjCheckNAdd(37);
                  break;
               case 38:
                  if ((0x3ff000000000000L & l) == 0L)
                     break;
                  if (kind > 8)
                     kind = 8;
                  jjCheckNAddTwoStates(38, 39);
                  break;
               case 40:
                  if ((0x280000000000L & l) != 0L)
                     jjCheckNAdd(41);
                  break;
               case 41:
                  if ((0x3ff000000000000L & l) == 0L)
                     break;
                  if (kind > 8)
                     kind = 8;
                  jjCheckNAdd(41);
                  break;
               default : break;
            }
         } while(i != startsAt);
      }
      else if (curChar < 128)
      {
         long l = 1L << (curChar & 077);
         MatchLoop: do
         {
            switch(jjstateSet[--i])
            {
               case 5:
               case 24:
                  if ((0x7fffffe07fffffeL & l) == 0L)
                     break;
                  if (kind > 65)
                     kind = 65;
                  jjCheckNAddTwoStates(24, 25);
                  break;
               case 42:
                  if ((0x7fffffe87fffffeL & l) != 0L)
                  {
                     if (kind > 65)
                        kind = 65;
                     jjCheckNAdd(25);
                  }
                  if ((0x7fffffe07fffffeL & l) != 0L)
                  {
                     if (kind > 65)
                        kind = 65;
                     jjCheckNAddTwoStates(24, 25);
                  }
                  break;
               case 1:
                  jjAddStates(10, 12);
                  break;
               case 7:
                  jjCheckNAddTwoStates(7, 8);
                  break;
               case 9:
               case 10:
                  jjCheckNAddTwoStates(10, 8);
                  break;
               case 15:
                  if ((0x2000000020L & l) != 0L)
                     jjAddStates(19, 20);
                  break;
               case 19:
                  jjCheckNAddStates(7, 9);
                  break;
               case 22:
                  jjCheckNAddStates(16, 18);
                  break;
               case 25:
                  if ((0x7fffffe87fffffeL & l) == 0L)
                     break;
                  if (kind > 65)
                     kind = 65;
                  jjCheckNAdd(25);
                  break;
               case 31:
                  if ((0x2000000020L & l) != 0L)
                     jjAddStates(21, 22);
                  break;
               case 35:
                  if ((0x2000000020L & l) != 0L)
                     jjAddStates(23, 24);
                  break;
               case 39:
                  if ((0x2000000020L & l) != 0L)
                     jjAddStates(25, 26);
                  break;
               default : break;
            }
         } while(i != startsAt);
      }
      else
      {
         int i2 = (curChar & 0xff) >> 6;
         long l2 = 1L << (curChar & 077);
         MatchLoop: do
         {
            switch(jjstateSet[--i])
            {
               case 1:
                  if ((jjbitVec0[i2] & l2) != 0L)
                     jjAddStates(10, 12);
                  break;
               case 7:
                  if ((jjbitVec0[i2] & l2) != 0L)
                     jjCheckNAddTwoStates(7, 8);
                  break;
               case 9:
               case 10:
                  if ((jjbitVec0[i2] & l2) != 0L)
                     jjCheckNAddTwoStates(10, 8);
                  break;
               case 19:
                  if ((jjbitVec0[i2] & l2) != 0L)
                     jjCheckNAddStates(7, 9);
                  break;
               case 22:
                  if ((jjbitVec0[i2] & l2) != 0L)
                     jjCheckNAddStates(16, 18);
                  break;
               default : break;
            }
         } while(i != startsAt);
      }
      if (kind != 0x7fffffff)
      {
         jjmatchedKind = kind;
         jjmatchedPos = curPos;
         kind = 0x7fffffff;
      }
      ++curPos;
      if ((i = jjnewStateCnt) == (startsAt = 42 - (jjnewStateCnt = startsAt)))
         return curPos;
      try { curChar = input_stream.readChar(); }
      catch(java.io.IOException e) { return curPos; }
   }
}
static final int[] jjnextStates = {
   27, 28, 29, 34, 35, 38, 39, 19, 21, 23, 1, 2, 4, 8, 9, 11, 
   21, 22, 23, 16, 17, 32, 33, 36, 37, 40, 41, 
};
public static final String[] jjstrLiteralImages = {
"", null, null, null, null, null, null, null, null, null, null, 
"\141\154\154", "\141\156\144", "\157\162", "\156\157\164", "\163\145\154\145\143\164", 
"\146\162\157\155", "\167\150\145\162\145", "\157\162\144\145\162", "\147\162\157\165\160", 
"\155\141\170", "\155\151\156", "\150\141\166\151\156\147", 
"\144\151\163\164\151\156\143\164", "\151\156", "\145\170\151\163\164\163", "\143\157\165\156\164", 
"\141\163\143", "\144\145\163\143", "\163\165\155", "\142\171", 
"\142\145\164\167\145\145\156", "\143\162\145\141\164\145", "\151\156\163\145\162\164", 
"\164\141\142\154\145", "\166\141\154\165\145\163", "\151\156\164\157", "\144\162\157\160", 
"\144\145\163\143\162\151\142\145", "\143\141\164\141\154\157\147", "\56", "\54", "\74", "\74\75", "\76", 
"\76\75", "\75", "\41\75", "\74\76", "\50", "\51", "\52", "\57", "\53", "\55", "\77", 
"\45", "\151\156\164\145\147\145\162", "\154\157\156\147", 
"\143\150\141\162\141\143\164\145\162", "\142\171\164\145", "\163\150\157\162\164", "\144\157\165\142\154\145", 
"\146\154\157\141\164", "\163\164\162\151\156\147", null, null, null, };
public static final String[] lexStateNames = {
   "DEFAULT", 
};
static final long[] jjtoToken = {
   0xfffffffffffffd81L, 0x3L, 
};
static final long[] jjtoSkip = {
   0x7eL, 0x0L, 
};
static protected SimpleCharStream input_stream;
static private final int[] jjrounds = new int[42];
static private final int[] jjstateSet = new int[84];
static protected char curChar;
public SQLParserTokenManager(SimpleCharStream stream){
   if (input_stream != null)
      throw new TokenMgrError("ERROR: Second call to constructor of static lexer. You must use ReInit() to initialize the static variables.", TokenMgrError.STATIC_LEXER_ERROR);
   input_stream = stream;
}
public SQLParserTokenManager(SimpleCharStream stream, int lexState){
   this(stream);
   SwitchTo(lexState);
}
static public void ReInit(SimpleCharStream stream)
{
   jjmatchedPos = jjnewStateCnt = 0;
   curLexState = defaultLexState;
   input_stream = stream;
   ReInitRounds();
}
static private final void ReInitRounds()
{
   int i;
   jjround = 0x80000001;
   for (i = 42; i-- > 0;)
      jjrounds[i] = 0x80000000;
}
static public void ReInit(SimpleCharStream stream, int lexState)
{
   ReInit(stream);
   SwitchTo(lexState);
}
static public void SwitchTo(int lexState)
{
   if (lexState >= 1 || lexState < 0)
      throw new TokenMgrError("Error: Ignoring invalid lexical state : " + lexState + ". State unchanged.", TokenMgrError.INVALID_LEXICAL_STATE);
   else
      curLexState = lexState;
}

static protected Token jjFillToken()
{
   Token t = Token.newToken(jjmatchedKind);
   t.kind = jjmatchedKind;
   String im = jjstrLiteralImages[jjmatchedKind];
   t.image = (im == null) ? input_stream.GetImage() : im;
   t.beginLine = input_stream.getBeginLine();
   t.beginColumn = input_stream.getBeginColumn();
   t.endLine = input_stream.getEndLine();
   t.endColumn = input_stream.getEndColumn();
   return t;
}

static int curLexState = 0;
static int defaultLexState = 0;
static int jjnewStateCnt;
static int jjround;
static int jjmatchedPos;
static int jjmatchedKind;

public static Token getNextToken() 
{
  int kind;
  Token specialToken = null;
  Token matchedToken;
  int curPos = 0;

  EOFLoop :
  for (;;)
  {   
   try   
   {     
      curChar = input_stream.BeginToken();
   }     
   catch(java.io.IOException e)
   {        
      jjmatchedKind = 0;
      matchedToken = jjFillToken();
      return matchedToken;
   }

   try { input_stream.backup(0);
      while (curChar <= 32 && (0x100002600L & (1L << curChar)) != 0L)
         curChar = input_stream.BeginToken();
   }
   catch (java.io.IOException e1) { continue EOFLoop; }
   jjmatchedKind = 0x7fffffff;
   jjmatchedPos = 0;
   curPos = jjMoveStringLiteralDfa0_0();
   if (jjmatchedKind != 0x7fffffff)
   {
      if (jjmatchedPos + 1 < curPos)
         input_stream.backup(curPos - jjmatchedPos - 1);
      if ((jjtoToken[jjmatchedKind >> 6] & (1L << (jjmatchedKind & 077))) != 0L)
      {
         matchedToken = jjFillToken();
         return matchedToken;
      }
      else
      {
         continue EOFLoop;
      }
   }
   int error_line = input_stream.getEndLine();
   int error_column = input_stream.getEndColumn();
   String error_after = null;
   boolean EOFSeen = false;
   try { input_stream.readChar(); input_stream.backup(1); }
   catch (java.io.IOException e1) {
      EOFSeen = true;
      error_after = curPos <= 1 ? "" : input_stream.GetImage();
      if (curChar == '\n' || curChar == '\r') {
         error_line++;
         error_column = 0;
      }
      else
         error_column++;
   }
   if (!EOFSeen) {
      input_stream.backup(1);
      error_after = curPos <= 1 ? "" : input_stream.GetImage();
   }
   throw new TokenMgrError(EOFSeen, curLexState, error_line, error_column, error_after, curChar, TokenMgrError.LEXICAL_ERROR);
  }
}

}
/* Generated By:JavaCC: Do not edit this line. Token.java Version 3.0 */

/**
 * Describes the input token stream.
 */

class Token {

  /**
   * An integer that describes the kind of this token.  This numbering
   * system is determined by JavaCCParser, and a table of these numbers is
   * stored in the file ...Constants.java.
   */
  public int kind;

  /**
   * beginLine and beginColumn describe the position of the first character
   * of this token; endLine and endColumn describe the position of the
   * last character of this token.
   */
  public int beginLine, beginColumn, endLine, endColumn;

  /**
   * The string image of the token.
   */
  public String image;

  /**
   * A reference to the next regular (non-special) token from the input
   * stream.  If this is the last token from the input stream, or if the
   * token manager has not read tokens beyond this one, this field is
   * set to null.  This is true only if this token is also a regular
   * token.  Otherwise, see below for a description of the contents of
   * this field.
   */
  public Token next;

  /**
   * This field is used to access special tokens that occur prior to this
   * token, but after the immediately preceding regular (non-special) token.
   * If there are no such special tokens, this field is set to null.
   * When there are more than one such special token, this field refers
   * to the last of these special tokens, which in turn refers to the next
   * previous special token through its specialToken field, and so on
   * until the first special token (whose specialToken field is null).
   * The next fields of special tokens refer to other special tokens that
   * immediately follow it (without an intervening regular token).  If there
   * is no such token, this field is null.
   */
  public Token specialToken;

  /**
   * Returns the image.
   */
  public String toString()
  {
     return image;
  }

  /**
   * Returns a new Token object, by default. However, if you want, you
   * can create and return subclass objects based on the value of ofKind.
   * Simply add the cases to the switch for all those special cases.
   * For example, if you have a subclass of Token called IDToken that
   * you want to create if ofKind is ID, simlpy add something like :
   *
   *    case MyParserConstants.ID : return new IDToken();
   *
   * to the following switch statement. Then you can cast matchedToken
   * variable to the appropriate type and use it in your lexical actions.
   */
  public static final Token newToken(int ofKind)
  {
     switch(ofKind)
     {
       default : return new Token();
     }
  }

}
/* Generated By:JavaCC: Do not edit this line. TokenMgrError.java Version 3.0 */

class TokenMgrError extends Error
{
   /*
    * Ordinals for various reasons why an Error of this type can be thrown.
    */

   /**
    * Lexical error occured.
    */
   static final int LEXICAL_ERROR = 0;

   /**
    * An attempt wass made to create a second instance of a static token manager.
    */
   static final int STATIC_LEXER_ERROR = 1;

   /**
    * Tried to change to an invalid lexical state.
    */
   static final int INVALID_LEXICAL_STATE = 2;

   /**
    * Detected (and bailed out of) an infinite loop in the token manager.
    */
   static final int LOOP_DETECTED = 3;

   /**
    * Indicates the reason why the exception is thrown. It will have
    * one of the above 4 values.
    */
   int errorCode;

   /**
    * Replaces unprintable characters by their espaced (or unicode escaped)
    * equivalents in the given string
    */
   protected static final String addEscapes(String str) {
      StringBuffer retval = new StringBuffer();
      char ch;
      for (int i = 0; i < str.length(); i++) {
        switch (str.charAt(i))
        {
           case 0 :
              continue;
           case '\b':
              retval.append("\\b");
              continue;
           case '\t':
              retval.append("\\t");
              continue;
           case '\n':
              retval.append("\\n");
              continue;
           case '\f':
              retval.append("\\f");
              continue;
           case '\r':
              retval.append("\\r");
              continue;
           case '\"':
              retval.append("\\\"");
              continue;
           case '\'':
              retval.append("\\\'");
              continue;
           case '\\':
              retval.append("\\\\");
              continue;
           default:
              if ((ch = str.charAt(i)) < 0x20 || ch > 0x7e) {
                 String s = "0000" + Integer.toString(ch, 16);
                 retval.append("\\u" + s.substring(s.length() - 4, s.length()));
              } else {
                 retval.append(ch);
              }
              continue;
        }
      }
      return retval.toString();
   }

   /**
    * Returns a detailed message for the Error when it is thrown by the
    * token manager to indicate a lexical error.
    * Parameters : 
    *    EOFSeen     : indicates if EOF caused the lexicl error
    *    curLexState : lexical state in which this error occured
    *    errorLine   : line number when the error occured
    *    errorColumn : column number when the error occured
    *    errorAfter  : prefix that was seen before this error occured
    *    curchar     : the offending character
    * Note: You can customize the lexical error message by modifying this method.
    */
   protected static String LexicalError(boolean EOFSeen, int lexState, int errorLine, int errorColumn, String errorAfter, char curChar) {
      return("Lexical error at line " +
           errorLine + ", column " +
           errorColumn + ".  Encountered: " +
           (EOFSeen ? "<EOF> " : ("\"" + addEscapes(String.valueOf(curChar)) + "\"") + " (" + (int)curChar + "), ") +
           "after : \"" + addEscapes(errorAfter) + "\"");
   }

   /**
    * You can also modify the body of this method to customize your error messages.
    * For example, cases like LOOP_DETECTED and INVALID_LEXICAL_STATE are not
    * of end-users concern, so you can return something like : 
    *
    *     "Internal Error : Please file a bug report .... "
    *
    * from this method for such cases in the release version of your parser.
    */
   public String getMessage() {
      return super.getMessage();
   }

   /*
    * Constructors of various flavors follow.
    */

   public TokenMgrError() {
   }

   public TokenMgrError(String message, int reason) {
      super(message);
      errorCode = reason;
   }

   public TokenMgrError(boolean EOFSeen, int lexState, int errorLine, int errorColumn, String errorAfter, char curChar, int reason) {
      this(LexicalError(EOFSeen, lexState, errorLine, errorColumn, errorAfter, curChar), reason);
   }
}
/*
 * Created on Jan 4, 2009 by sviglas
 *
 * This is part of the attica project.  Any subsequent modification
 * of the file should retain this disclaimer.
 * 
 * University of Edinburgh, School of Informatics
 */


/**
 * Wrapper for a page in the buffer pool, keeping track of its
 * dirtyness.
 *
 * @author sviglas
 */
class BufferedPage {

    public Page page;
    public boolean dirty;

    public BufferedPage(Page page) {
        this.page = page;
        dirty = false;
    } // BufferedPage()

    
} // BufferedPage
/*
 * Created on Nov 25, 2003 by sviglas
 *
 * Modified on Dec 20, 2008 by sviglas
 *
 * Heavily modified on Jan 4, 2009 by sviglas
 *
 * This is part of the attica project.  Any subsequent modification
 * of the file should retain this disclaimer.
 * 
 * University of Edinburgh, School of Informatics
 */



/**
 * BufferManager: The basic abstraction of attica's buffer manager.
 *
 * @author sviglas
 */
class BufferManager {
	
    /** The number of pages stored in this buffer manager. */
    private int numPages;
	
    /** The pages stored in this buffer manager. */
    private BufferedPage [] pages;
	
    /** Maps a page id to an index in the page array. */
    private Map<PageIdentifier, Integer> idToIdx;

    /** The current index in the pool. */
    private int currentIndex;

    /** The LRU replacement queue. */
    private LinkedList<PageIdentifier> lruQueue;
    
    /**
     * Create a new buffer manager given the number of pages the
     * buffer manager should hold.
     * 
     * @param numPages this buffer manager's number of pages
     */
    public BufferManager(int numPages) {
        
        this.numPages = numPages;
        currentIndex = 0;
        pages = new BufferedPage[numPages];
        idToIdx = new HashMap<PageIdentifier, Integer>();
        // you may be asking: are you stupid enough to organise the
        // LRU queue on identifiers rather than indexes? well, I'm
        // not. but java is stupid enough to autobox/unbox all
        // integers in a generic list of integers and this may lead to
        // some problems, to put it mildly (since there is no decent
        // queue implementation, and we're actually using a linked
        // list)
        // 
        lruQueue = new LinkedList<PageIdentifier>();
    } // BufferManager()

    
    /**
     * The number of pages stored in this buffer manager.
     * 
     * @return this buffer manager's number of pages.
     */
    public int getNumberOfPages() {
        return numPages;
    } // getNumberOfPages()

    
    /**
     * Is the page specified by the page id in the buffer pool
     * or not?
     * 
     * @param pageid the id of the page searched for.
     * @return <pre>true</pre> if the page is in the buffer pool
     * <pre>false</pre> otherwise.
     */
    public boolean containsPage(PageIdentifier pageid) {
        return getIndex(pageid) != -1;
    } // containsPage()

    
    /**
     * Returns a page given a page id.
     * 
     * @param pageid the id of the page to be returned
     * @return the page that corresponds to the given page id
     */
    public Page getPage(PageIdentifier pageid) {
        
        int index = getIndex(pageid);
        if (index >= 0) {
            lruQueue.remove(pageid);
            lruQueue.add(pageid);
            return pages[index].page;
        }
        return null;
    } // getPage()


    /**
     * Returns the index of a page in the pool given its identifier.
     *
     * @return the index of a page in the pool, or -1 if the page is
     * not there
     */
    protected int getIndex(PageIdentifier pageid) {
        
        Integer idx = idToIdx.get(pageid);
        return (idx == null ? -1 : idx);
    } // getIndex()

    
    /**
     * Touches the page corresponding to the pageid making it dirty
     * and moving it to the back of the replacement queue.
     * 
     * @param pageid the pageid of the page to be touched.
     */
    public void touchPage(PageIdentifier pageid) { 

        if (containsPage(pageid)) {
            int index = getIndex(pageid);
            pages[index].dirty = true;
            lruQueue.remove(pages[index].page.getPageIdentifier());
            lruQueue.add(pages[index].page.getPageIdentifier());
        }
    } // touchPage()

    
    /**
     * Touches a page in the buffer pool (if it exists) making it dirty
     * and setting its timestamp.
     * 
     * @param page the page to be touched.
     */
    public void touchPage(Page page) {
        touchPage(page.getPageIdentifier());
    } // touchPage()


    /**
     * Wrapper for putting a page in the buffer pool, assuming the
     * page is dirty.
     *
     * @param page the page to be inserted.
     * @return the page to be evicted (if any).
     */
    public Page putPage(Page page) {
        return putPage(page, true);
    } // putPage()
    
    
    /**
     * Puts a page in the buffer pool.
     * 
     * @param page the page to be inserted into the buffer pool.
     * @param dirty is this page dirty or not?
     * @return the page to be evicted (if any).
     */
    public Page putPage(Page page, boolean dirty) {
        
        int index; 
        // if the page is in the buffer pool
        if (containsPage(page.getPageIdentifier())) {
            index = getIndex(page.getPageIdentifier());
            pages[index].page = page;
            pages[index].dirty = dirty;
            lruQueue.remove(page.getPageIdentifier());
            lruQueue.add(page.getPageIdentifier());
            return null;
        }
        // if the page is not in the buffer pool, but the buffer
        // pool is not full
        else if (! isFull()) {
            index = currentIndex++;
            idToIdx.put(page.getPageIdentifier(), index);
            pages[index] = new BufferedPage(page);
            pages[index].dirty = dirty;
            lruQueue.add(page.getPageIdentifier());
            return null;
        }
        // if the page is not in the buffer pool and the buffer
        // pool is full
        else {
            index = indexToEvict();
            Page pageToFlush = pages[index].page;
            idToIdx.remove(pageToFlush.getPageIdentifier());
            idToIdx.put(page.getPageIdentifier(), index);
            pages[index].dirty = dirty;
            pages[index].page = page;
            lruQueue.add(page.getPageIdentifier());
            return pageToFlush;
        }
    } // putPage()

    
    /**
     * Is the buffer manager full or not?
     * 
     * @return <pre>true</pre> if the buffer manager is full
     * <pre>false</pre> otherwise.
     */
    protected boolean isFull() {
        return currentIndex == numPages;
    } // isFull()

    
    /**
     * Identify the page to be evicted from the buffer pool to make
     * room.
     * 
     * @return the index of the page that should be evicted.
     */
    protected int indexToEvict() {
        PageIdentifier pid = lruQueue.removeFirst();
        return idToIdx.get(pid);
        /*
         //
         // left here for posterity as a token of everlasting
         // stupidity
         //
        int index = 0;
        Date latest = pages[0].timestamp;
        int i = 0;
        for (BufferedPage page : pages) {
            if (page.timestamp.compareTo(latest) < 0) {
                index = i;
                latest = page.timestamp;
            }
            i++;
        }	
        return index;
        */
    } // indexToEvict()


    /**
     * Invalidate all buffer pool entries for a specific file.
     *
     * @param fn the filename.     
     */
    public void invalidate(String fn) {
        
        for (int index = 0; index < currentIndex;) {
            PageIdentifier pid = pages[index].page.getPageIdentifier();
            if (pid.getFileName().equals(fn)) {
                System.arraycopy(pages, index+1, pages, index,
                                 currentIndex-index-1);
                idToIdx.remove(pid);
                lruQueue.remove(pid);
                // update all page references in the id to index map
                for (Map.Entry<PageIdentifier, Integer> e :
                         idToIdx.entrySet()) {
                    // autoboxing doesn't always work, so just autobox
                    // it before access, just in case
                    int v = e.getValue();
                    if (v > index) e.setValue(v-1);
                }
                currentIndex--;
            }
            else {
                index++;
            }
        }
    } // invalidate()

    
    /**
     * Returns an iterable over the pages of the buffer pool.
     *
     * @return an iterable over the pages of the buffer pool.
     */
    Iterable<Page> pages() {
        return new PageIteratorWrapper();
    } // pages()


    /**
     * Inner iterator for the pages of the buffer pool.
     */
    class PageIteratorWrapper implements Iterable<Page> {
        private int index;
        public PageIteratorWrapper() { index = 0; }

        public Iterator<Page> iterator() {
            return new Iterator<Page>() {
                public boolean hasNext() { return index < currentIndex; }
                public Page next() { return pages[index++].page; }
                public void remove() {
                    throw new UnsupportedOperationException();
                }
            }; // new Iterator
        } // iterator()
    } // PageIteratorWrapper

} // BufferManager
/*
 * Created on Oct 10, 2003 by sviglas
 *
 * This is part of the attica project.  Any subsequent modification
 * of the file should retain this disclaimer.
 * 
 * University of Edinburgh, School of Informatics
 */



/**
 * Catalog: The catalog abstraction of the attica storage manager.
 *
 * (By the way, I know this implementation is the lamest, most
 * brain-damaged one I could come up with.  I'm just too lazy to
 * bootstrap the database catalog properly.)
 *
 * @author sviglas
 */
class Catalog {
	
    /** The name of the catalog file. */
    private String catalogFile;
	
    /** The entries of the catalog, mapping table names to entries. */
    private Map<String, CatalogEntry> entries;
    
    /** The number of tables currently stored in the catalog. */
    private int numberOfTables;

    
    /**
     * Construct a new catalog given the name of the catalog file.
     * 
     * @param catalogFile the name of the catalog file.
     */
    public Catalog(String catalogFile) {
        
        this.catalogFile = catalogFile;
        entries = new Hashtable<String, CatalogEntry>();
    } // Catalog()
    
    
    /**
     * Creates an entry into the catalog for a new table.
     * 
     * @param entry the catalog entry for the new catalog.
     * @throws IllegalArgumentException thrown whenever the user
     * tries to create a table with the same name as an existing one.
     */
    public void createNewEntry(CatalogEntry entry) 
	throws IllegalArgumentException {
        
        if (! tableExists(entry.getTableName())) {
            entries.put(entry.getTableName(), entry);
            numberOfTables++;
        }
        else {
            throw new IllegalArgumentException("Table: "
                                               + entry.getTableName()
                                               + " already exists.");
        }
    } // createNewEntry()

    
    /**
     * Checks to see whether the given table exists in the catalog.
     * 
     * @param tableName the name of the table to be checked.
     * @return <pre>true</pre> if the table exists, <pre>false</pre>
     * otherwise.
     */
    protected boolean tableExists(String tableName) {
        return (entries.get(tableName) != null);
    } // tableExists()

    
    /**
     * Reads in the catalog from the file specified by the filename
     * passed as an argument to the constructor.
     * 
     * @throws StorageManagerException thrown whenever there is
     * something wrong with reading the catalog file
     */
    @SuppressWarnings("unchecked")
    public void readCatalog() throws StorageManagerException {
        
        
        try {            
            FileInputStream fstream = new FileInputStream(catalogFile);
            ObjectInputStream istream = new ObjectInputStream(fstream);
            
            // read in the number of tables
            numberOfTables = istream.readInt();
            // read in the entries of the catalog
            
            // OK, mini-rant time: generics are brain-damaged in this
            // respect. my guess is that it's using the erased type
            // for the safety check, so there's no way to find the
            // instantiated type. makes sense, but it just shows you
            // that the way generics have been implemented loses all
            // type information after compilation and it's just
            // syntactic sugar for type safety -- which cannot be
            // guaranteed across invocations of the same code. yeah,
            // well, **** me gently with a chainsaw...
            entries = (Map<String, CatalogEntry>) istream.readObject();
            
            istream.close();
        }
        catch (ClassCastException cce) {
            throw new StorageManagerException("Could not cast from catalog "
                                              + "file.", cce);
        }
        catch (ClassNotFoundException cnfe) {
            throw new StorageManagerException("Casting error when reading the "
                                              + "catalog entries from file "
                                              + catalogFile
                                              + ". Could not cast to"
                                              + "entry type.", cnfe);
        }
        catch (FileNotFoundException fnfe) {
            throw new StorageManagerException("*CAUTION* catalog file "
                                              + catalogFile + " not found!",
                                              fnfe);
        }
        catch (IOException ioe) {
            throw new StorageManagerException("I/O Exception while opening "
                                              + "the catalog from file "
                                              + catalogFile, ioe);
        }
    } // readCatalog()

    
    /**
     * Writes the catalog into the specified file specified by the
     * filename passed as an argument to the constructor.
     * 
     * @throws StorageManagerException thrown whenever there is
     * something wrong with writing out the catalog file.
     */
    public void writeCatalog() throws StorageManagerException {
        
        try {
            FileOutputStream fstream = new FileOutputStream(catalogFile);
            ObjectOutputStream ostream = new ObjectOutputStream(fstream);
            
            // write out the number of tables
            ostream.writeInt(numberOfTables);
            // write out the entries of the catalog
            ostream.writeObject(entries);
            
            ostream.flush();
            ostream.close(); 
        }
        catch (IOException ioe) {
            throw new StorageManagerException("I/O Exception while storing "
                                              + "the catalog file in "
                                              + catalogFile, ioe);
        }
    } // writeCatalog()

    
    /**
     * Returns the name of the file associated with a table.
     * 
     * @param tableName the table name.
     * @return the name of the file associated with a table.
     * @throws NoSuchElementException if a table with the given file
     * name does not exist in the catalog.
     */
    public String getTableFileName(String tableName) 
	throws NoSuchElementException {
        
        CatalogEntry entry = entries.get(tableName);
        if (entry == null) 
            throw new NoSuchElementException("Table " + tableName + " is not "
                                             + "in the DB catalog.");
        else
            return entry.getFileName();
    } // getTableFileName()

    
    /**
     * Returns the table associated with a table name.
     * 
     * @param tableName the table name
     * @return the table associated with the table name
     * @throws NoSuchElementException whenever the given table does
     * not exist.
     */
    public Table getTable(String tableName) 
	throws NoSuchElementException {
        
        CatalogEntry entry = (CatalogEntry) entries.get(tableName);
        if (entry == null) 
            throw new NoSuchElementException("Table " + tableName + " is not "
                                             + "in the DB catalog.");
        else
            return entry.getTable();
    } // getTable()

    
    /**
     * Deletes a table from the catalog.
     * 
     * @param tablename the name of the table to be deleted.
     * @throws NoSuchElementException thrown if the table is not in
     * the catalog.
     */
    public void deleteTable(String tablename)
        throws NoSuchElementException {
        
        if (! tableExists(tablename))
            throw new NoSuchElementException("Table " + tablename + " is not "
                                             + "in the DB catalog.");
        entries.remove(tablename);
        numberOfTables--;
    } // deleteTable()

    
    /**
     * Returns the string representation of this catalog.
     * 
     * @return the catalog as a string.
     */
    @Override
    public String toString() {
        return "Catalog: " + numberOfTables + " tables.  Entries: \n"
            + "\t" + entries.toString(); 
    } // toString()

    
    /**
     * Debug main()
     */
    public static void main (String args[]) {
        try {
            // create three new table entries
            CatalogEntry ce1 =
                new CatalogEntry(new Table("sailors",
                                           new ArrayList<Attribute>()));
            CatalogEntry ce2 =
                new CatalogEntry(new Table("boats",
                                           new ArrayList<Attribute>()));
            CatalogEntry ce3 =
                new CatalogEntry(new Table("trips",
                                           new ArrayList<Attribute>()));
            
            // create the catalog
            Catalog catalog = new Catalog(args[0]);
            
            // add info to the catalog
            catalog.createNewEntry(ce1);
            catalog.createNewEntry(ce2);
            catalog.createNewEntry(ce3);
            
            // print out the catalog
            System.out.println(catalog);
            
            // write it out
            System.out.println("Flushing catalog.");
            catalog.writeCatalog();
            
            // read it back in
            System.out.println("Reading catalog.");
            catalog.readCatalog();
            
            // print it out again
            System.out.println(catalog);
        }
        catch (Exception e) {
            System.err.println("Exception " + e.getMessage());
            e.printStackTrace(System.err);
        }
    } // main()

} // Catalog
/*
 * Created on Nov 25, 2003 by sviglas
 *
 * This is part of the attica project.  Any subsequent modification
 * of the file should retain this disclaimer.
 * 
 * University of Edinburgh, School of Informatics
 */



/**
 * CatalogEntry: Stores all information pertinent to a table in the
 * catalog.
 *
 * @author sviglas
 */
class CatalogEntry implements Serializable {
	
    /** The table for this catalog entry. */
    private Table table;
	
    /** The filename of the table. */
    private String fileName;

    
    /**
     * Creates a new catalog entry given the table.
     * 
     * @param table the table name for this catalog entry.
     */
    public CatalogEntry(Table table) {
        
        this.table = table;
        createFileName();
    } // CatalogEntry()

    
    /**
     * The name of the table this entry corresponds to.
     * 
     * @return this catalog entry's table name.
     */
    public String getTableName() {
        return table.getName();
    } // getTableName()

    
    /**
     * Returns this catalog entry's table.
     * 
     * @return the table this catalog entry corresponds to.
     */
    public Table getTable() {
        return table;
    } // getTable()

    
    /**
     * Returns the filename of this catalog entry's table's filename.
     * 
     * @return This catalog entry's table's filename
     */
    public String getFileName() {
        return fileName;
    } // getFileName()

    
    /**
     * Creates a new filename for the entry's table name.
     */
    protected void createFileName() {
        
        String tableName = table.getName();
        fileName = new String(org.dejave.attica.server.Database.ATTICA_DIR
                              + System.getProperty("file.separator")
                              + tableName + "_" + tableName.hashCode());
    } // createFileName()

    
    /**
     * String representation.
     * 
     * @return this entry's string representation.
     */
    @Override
    public String toString() {
        return "Table: " + getTableName() + ", filename: "
            + fileName + ", def: " + table;
    } // toString()

} // CatalogEntry
/*
 * Created on Dec 5, 2003 by sviglas
 *
 * Modified on Dec 18, 2008 by sviglas
 *
 * This is part of the attica project.  Any subsequent modification
 * of the file should retain this disclaimer.
 * 
 * University of Edinburgh, School of Informatics
 */


/**
 * DatabaseFile: A paged database file.
 *
 * @author sviglas
 */
class DatabaseFile extends RandomAccessFile {
    
    /** Constant signifying read access to a file */
    public static final String READ = "r";
    
    /** Constant signifying read/write access to a file. */
    public static final String READ_WRITE = "rw";
    
    /**
     * Constructs a new DatabaseFile instance.
     * 
     * @param filename the name of the file.
     * @param mode the mode in which the file is to be opened.
     * @throws FileNotFoundException thrown whenever the file is not
     * found.
     */
    public DatabaseFile(String filename, String mode) 
	throws FileNotFoundException {
        super(filename, mode);
    } // DatabaseFile()
    
} // DatabaseFile
/*
 * Created on Dec 7, 2003 by sviglas
 *
 * Modified on Dec 17, 2008 by sviglas
 *
 * This is part of the attica project.  Any subsequent modification
 * of the file should retain this disclaimer.
 * 
 * University of Edinburgh, School of Informatics
 */


/**
 * FileUtil: Basic file utilities.
 *
 * @author sviglas
 */
class FileUtil {

    /** Counter for files. */
    private static long next = 0;
    
    /**
     * Returns the size (in bytes) of the file corresponding to the
     * given filename.
     * 
     * @param filename the name of the file to be checked.
     * @return the size (in bytes) of the file corresponding to the
     * given filename.
     * @throws IOException generic I/O exception.
     * @throws FileNotFoundException thrown whenever the file does not
     * exist.
     */
    public static long getFileSize(String filename) 
	throws IOException, FileNotFoundException {
        
        DatabaseFile dbf = new DatabaseFile(filename, DatabaseFile.READ);
        long size = dbf.length();
        dbf.close();
        return size;
    } // getFileSize()

    
    /**
     * Return the number of attica pages in the file corresponding to
     * the given filename.
     * 
     * @param filename the name of the file to be checked.
     * @return the number of attica pages in the file corresponding to the
     * given filename.
     * @throws IOException generic I/O exception.
     * @throws FileNotFoundException thrown whenever the file does not
     * exist.
     */
    public static int getNumberOfPages(String filename) 
	throws IOException, FileNotFoundException {
        
        DatabaseFile dbf = new DatabaseFile(filename, DatabaseFile.READ);
        int pages = (int) (dbf.length() / Sizes.PAGE_SIZE + .5);
        dbf.close();
        return pages;
    } // getNumberOfPages()


    public static void setNumberOfPages(String filename, int np)
        throws IOException, FileNotFoundException {

        DatabaseFile dbf = new DatabaseFile(filename, DatabaseFile.READ_WRITE);
        int pages = (int) (dbf.length() / Sizes.PAGE_SIZE + .5);
        if (pages < np) dbf.setLength(np*Sizes.PAGE_SIZE);
        dbf.close();
    }
    
    /**
     * Create a new temporary file name.
     * 
     * @return a new temp file name.
     */
    public static String createTempFileName() {
        
        return org.dejave.attica.server.Database.TEMP_DIR
            +  System.getProperty("file.separator")
            + "attica-" + (next++) + ".tmp";
    } // createFileName()

} // FileUtil
/*
 * Created on Dec 9, 2003 by sviglas
 *
 * Modified on Dec 18, 2008 by sviglas
 *
 * This is part of the attica project.  Any subsequent modification
 * of the file should retain this disclaimer.
 * 
 * University of Edinburgh, School of Informatics
 */

/**
 * IntermediateTupleIdentifier: A unique identifier for an
 * intermediate tuple.
 *
 * @author sviglas
 */
class IntermediateTupleIdentifier extends TupleIdentifier {

    /**
     * Constructs a new identifier given its number.
     *
     * @param number the intermediated tuple's identifying number.
     */
    public IntermediateTupleIdentifier(int number) {
        super("", number);
    } // IntermediateTupleIdentifer()
    
} // IntermediateTupleIdentifer
/*
 * Created on Oct 9, 2003 by sviglas
 *
 * Modified on Dec 18, 2008 by sviglas
 * 
 * This is part of the attica project.  Any subsequent modification
 * of the file should retain this disclaimer.
 * 
 * University of Edinburgh, School of Informatics
 */



/**
 * Page: The basic representation of an attica page. NB: this is the
 * language representation -- disk pages will likely vary.
 *
 * @author sviglas
 */
class Page implements Iterable<Tuple> {
    
    /** The tuples of this page. */
    private List<Tuple> tuples;
	
    /** The ID of the page. */
    private PageIdentifier pageId;
	
    /** The relational schema this page conforms to. */
    private Relation relation;

    /** The free space in this page. */
    private int freeSpace;
    
    /**
     * Creates a new page given its schema and page identifier.
     * 
     * @param relation the relation this page conforms to.
     * @param pageId the ID of this page.
     */
    public Page(Relation relation, PageIdentifier pageId) {
        this.relation = relation;
        this.pageId = pageId;
        this.tuples = new ArrayList<Tuple>();
        freeSpace = Sizes.PAGE_SIZE - Convert.INT_SIZE;
    } // Page()
	

    /**
     * Returns the relation this page conforms to.
     * 
     * @return the relation this page belongs to.
     */
    public Relation getRelation() {
        return relation;
    } // getRelation()

    
    /**
     * Checks whether this page has room for one more tuple.
     *
     * @return <pre>true</pre> if there is room for one more tuple
     * on this page, <pre>false</pre> otherwise.
     */
    public boolean hasRoom(Tuple t) {
        return freeSpace >= TupleIOManager.byteSize(getRelation(), t);
    } // hasSpace()

    
    /**
     * Returns the number of occupied tuples of this page.
     *
     * @return the number of occupied tuples of this page.
     */
    public int getNumberOfTuples() {
        return tuples.size();
    } // getNumberOfTuples()

    
    /**
     * Sets the number of tuples in this page (package visible).
     *
     * @param numTuples the new number of tuples.     
     */
    /*
    void setNumberOfTuples(int numTuples) {
        this.numTuples = numTuples;
    } // setNumberOfTuples
    */
    
    /**
     * Adds a new tuple to the page.
     * 
     * @param tuple the new tuple.
     * @throws ArrayIndexOutOfBoundsException thrown whenever the page
     * boundaries are crossed.
     */
    public void addTuple(Tuple tuple) 
	throws ArrayIndexOutOfBoundsException {
        
        if (hasRoom(tuple)) {
            tuples.add(tuple);
            freeSpace -= TupleIOManager.byteSize(getRelation(), tuple);
        }
        else throw new ArrayIndexOutOfBoundsException("No more space in page.");
    } // addTupleToPage()

    
    /**
     * Sets the specified tuple.
     * 
     * @param index the index of the tuple to be changed.
     * @param tuple the new tuple.
     * @throws ArrayIndexOutOfBoundsException thrown whenever the page
     * boundaries are crossed.
     * @throws IllegalArgumentException if the new tuple does not fit
     * into the page.
     */
    public void setTuple(int index, Tuple tuple) 
	throws ArrayIndexOutOfBoundsException, IllegalArgumentException {

        if (! canSubstitute(index, tuple))
            throw new IllegalArgumentException("New tuple does not fit.");
        tuples.set(index, tuple);
    } // setTuple()


    /**
     * Checks whether the specified index can be substituted for the
     * new tuple.
     *
     * @param index the index of the tuple to be changed.
     * @param nt the new tuple.
     * @throws ArrayIndexOutOfBoundsException whenever the referenced
     * index does not exist.
     */
    public boolean canSubstitute(int index, Tuple nt)
        throws ArrayIndexOutOfBoundsException {

        return (freeSpace
            + TupleIOManager.byteSize(getRelation(), tuples.get(index))
                - TupleIOManager.byteSize(getRelation(), nt)) >= 0;
    } // canSubstitute()


    /**
     * Swaps two tuples by their indexes.
     *
     * @param x the first index.
     * @param y the second index.
     * @throws ArrayIndexOutOfBoundsException whenever any of the
     * referenced indexes do not exist.
     */
    public void swap(int x, int y) throws ArrayIndexOutOfBoundsException {
        Tuple t = tuples.get(x);
        tuples.set(x, tuples.get(y));
        tuples.set(y, t);
    } // swap()

    
    /**
     * Retrieves the specified tuple from the page.
     * 
     * @param index the index of the tuple to be retrieved.
     * @return the index-th tuple.
     * @throws ArrayIndexOutOfBoundsException thrown whenever there
     * is an error in the indexing.
     */
    public Tuple retrieveTuple(int index) 
	throws ArrayIndexOutOfBoundsException {
        
        return tuples.get(index);
    } // retrieveTuple()

    
    /**
     * Retrieves the ID of this page
     * 
     * @return the ID of the page
     */
    public PageIdentifier getPageIdentifier() {        
        return pageId;
    } // getPageIdentifier()

    
    /**
     * Returns an iterator over this page.
     *
     * @return an iterator over the tuples of this page.
     */
    public Iterator<Tuple> iterator() {
        return new PageIterator();
    }

    /**
     * The iterator over the tuples of this page. Doesn't wrap the
     * list iterator because we want to keep track of free space on
     * removal (and, yes, this is brain-damaged since the system
     * doesn't support deletions yet).
     */
    private class PageIterator implements Iterator<Tuple> {
        /** The current index of the iterator. */
        private int currentIndex;

        /**
         * Constructs a new iterator over this page's contents.
         */
        public PageIterator() {
            currentIndex = 0;
        }

        /**
         * Checks whether there are more tuples in this page.
         *
         * @return <code>true</code> if there are more tuples in this
         * page, <code>false</code> otherwise.
         */
        public boolean hasNext() {
            return currentIndex < tuples.size();
        }

        /**
         * Returns the next tuple of the iterator.
         *
         * @return the next tuple of the iterator.
         */
        public Tuple next() {
            return tuples.get(currentIndex++);
        }

        /**
         * Removes the last tuple returned by the iterator.
         */
        public void remove() {
            int size = TupleIOManager.byteSize(getRelation(),
                                               tuples.get(currentIndex));
            freeSpace += size;
            tuples.remove(currentIndex);
        }
    } // PageIterator()
    

    /**
     * Returns a textual representation of the page.
     * 
     * @return this page's textual representation.
     */
    @Override
    public String toString() {
        
        StringBuffer sb = new StringBuffer();
        sb.append("page: " + getPageIdentifier() + ", tuples: {\n");
        int tid = 0;
        for (Tuple it : this)
            sb.append("\t" + tid++ + ": " + it.toString() + "\n");
        sb.append("}");
        return sb.toString();
    } // toString()
    
} // Page
/*
 * Created on Nov 26, 2003 by sviglas
 *
 * This is part of the attica project.  Any subsequent modification
 * of the file should retain this disclaimer.
 * 
 * University of Edinburgh, School of Informatics
 */

/**
 * PageIdentifier: Identifies an attica page on disk.
 *
 * @author sviglas
 */
class PageIdentifier {
	
    /** The file this page belongs to. */
    private String fileName;
	
    /** The number of this page. */
    private int number;

    
    /**
     * Creates a new identifier.
     * 
     * @param fileName the file this page belongs to.
     * @param number the number of this page.
     */
    public PageIdentifier(String fileName, int number) {
        
        this.fileName = fileName;
        this.number = number;
    } // PageIdentifier()

    
    /**
     * Retrieves the name of the file this page belongs to.
     * 
     * @return the name of the file this page belongs to.
     */
    public String getFileName() {
        
        return fileName;
    } // getFileName()

    
    /**
     * Retrieves the number of this page.
     * 
     * @return the number of this page.
     */
    public int getNumber () {
        return number;
    } // getNumber()

    
    /**
     * Compares two identifiers for equality.
     * 
     * @param o the object this identifier is compared to
     * @return <pre>true</pre> if the two identifiers are equal,
     * <pre>false</pre> otherwise
     */
    @Override
    public boolean equals(Object o) {
        
        if (o == this) return true;        
        if (! (o instanceof PageIdentifier)) return false;
        PageIdentifier pi = (PageIdentifier) o;        
        return (getFileName() == null
                ? pi.getFileName() == null
                : getFileName().equals(pi.getFileName()))
            && getNumber() == pi.getNumber();
    } // equals()

    
    /**
     * Overrides hash code for equals() consistency.
     *
     * @return the hash code of this page identifier.
     */
    @Override
    public int hashCode() {

        // just DEK it and be done with it...
        int hash = 17;
        int code = (getFileName() != null ? getFileName().hashCode() : 0);
        hash = 31*hash + code;
        code = getNumber();
        hash = 31*hash + code;
        return hash;
    } // hashCode()

    
    /**
     * Returns a string representation of this identifier.
     * 
     * @return this identifier's string representation.
     */
    @Override
    public String toString() {
        
        return "[page " + getFileName() + ":" + getNumber() + "]";
    } // toString()

} // PageIdentifier
/*
 * Created on Dec 5, 2003 by sviglas
 *
 * Modofied on Dec 24, 2008 by sviglas
 *
 * This is part of the attica project.  Any subsequent modification
 * of the file should retain this disclaimer.
 * 
 * University of Edinburgh, School of Informatics
 */



/**
 * PageIOManager: Implements page I/O over attica files.
 *
 * @author sviglas
 */
class PageIOManager {
	
    /**
     * Constructs a new page I/O manager.
     */
    public PageIOManager() {} 
	
    /**
     * Writes a page to an output file.
     * 
     * @param raf the output file.
     * @param page the page to be written.
     * @throws StorageManagerException thrown whenever there is an
     * output error.
     */
    public static void writePage(RandomAccessFile raf, Page page) 
	throws StorageManagerException {
        
        try {
            // seek to the correct place in the file
            long seek = page.getPageIdentifier().getNumber() * Sizes.PAGE_SIZE;
            raf.seek(seek);
            byte [] bytes = new byte[Sizes.PAGE_SIZE];
            dumpNumber(page, bytes);
            dumpTuples(page, bytes);
            raf.write(bytes);
        }
        catch (IOException ioe) {
            throw new StorageManagerException("Exception while writing page "
                                              + page.getPageIdentifier()
                                              + " to disk.", ioe);
        }
    } // writePage()

    
    /**
     * Reads a page from disk.
     * 
     * @param relation the relation the requested page conforms to.
     * @param pid the page identifier of the page.
     * @param raf the output file.
     * @return the page read.
     * @throws StorageManagerException whenever the page cannot be
     * properly read.
     */
    public static Page readPage(Relation relation, PageIdentifier pid, 
                                RandomAccessFile raf) 
	throws StorageManagerException {
        
        try {
            // seek to the appropriate place
            long seek = pid.getNumber() * Sizes.PAGE_SIZE;
            raf.seek(seek);
            byte [] bytes = new byte[Sizes.PAGE_SIZE];
            int bytesRead = raf.read(bytes);
            if (bytesRead == -1) {
                // we've reached the end of file, so we need to
                // allocate a page
                raf.setLength(seek + Sizes.PAGE_SIZE);
                return new Page(relation, pid);
            }
            if (bytesRead != Sizes.PAGE_SIZE) {
                throw new StorageManagerException("Page " + pid.toString()
                                                  + "was not fully read.");
            }
            return fetchTuples(relation, pid, bytes);
        }
        catch (IOException ioe) {
            throw new StorageManagerException("Exception while reading page "
                                              + pid.toString()
                                              + " from disk.", ioe);
        }
    } // readPage()


    /**
     * Dumps the number of tuples of the page.
     *
     * @param page the page to be written.
     * @param bytes the output byte array.     
     */     
    protected static void dumpNumber(Page page, byte [] bytes) {
        
        byte [] b = Convert.toByte(page.getNumberOfTuples());
        System.arraycopy(b, 0, bytes, 0, b.length);
    } // dumpNumber()

    
    /**
     * Dump the page tuples to disk.
     * 
     * @param page the page to be written to disk.
     * @param bytes the output byte array for the tuples.
     * @throws StorageManagerException if the 
     */
    protected static void dumpTuples(Page page, byte [] bytes)
        throws StorageManagerException {
        
        // start up a new tuple IO manager
        TupleIOManager manager =
            new TupleIOManager (page.getRelation(),
                                page.getPageIdentifier().getFileName());
        // one integer was used for the number of tuples
        int offset = Convert.INT_SIZE;
        // iterate over all tuples, place them in the array
        for (Tuple tuple : page)
            offset = manager.writeTuple(tuple, bytes, offset);
        pad(bytes, offset);
    } // dumpTuples()
    
    /**
     * Reads in tuples from disk and puts them in a new page.
     * 
     * @param relation the relation the page conforms to.
     * @param pid the page identifier of the new page.
     * @param bytes the byte array where the tuples are in.
     * @return the page read from disk.
     */
    protected static Page fetchTuples(Relation relation, PageIdentifier pid, 
                                      byte [] bytes) 
	throws StorageManagerException {
        
        // start up a new tuple IO manager
        TupleIOManager manager = new TupleIOManager(relation,
                                                    pid.getFileName());
        // start reading tuples
        int numberOfTuples = fetchNumber(bytes);
        Page page = new Page(relation, pid);
        //page.setNumberOfTuples(numberOfTuples);
        // one integer for the number of tuples
        int offset = Convert.INT_SIZE;
        for (int i = 0; i < numberOfTuples; i++) {
            Pair pair = manager.readTuple(bytes, offset);
            Tuple tuple = (Tuple) pair.first;
            offset = ((Integer) pair.second).intValue();
            page.addTuple(tuple);
        }
		
        return page;
    } // fetchTuples()

    /**
     * Fetches the number of tuples from a byte array.
     *
     * @param bytes the byte array.
     * @return the number of tuples.
     */
    public static int fetchNumber(byte [] bytes) {
        
        byte [] b = new byte[Convert.INT_SIZE];
        System.arraycopy(bytes, 0, b, 0, b.length);
        return Convert.toInt(b);
    } // fetchNumber()

    
    /**
     * Pad a byte array with zeros to reach the disk page size.  (Not
     * sure this will ever be used, but whatever...)
     * 
     * @param bytes the input byte array to be padded.
     * @param start the starting offset in the byte array.
     */
    protected static void pad(byte [] bytes, int start) {
        for (int i = start; i < bytes.length; i++) bytes[i] = (byte) 0;        
    } // pad()

} // PageIOManager
/*
 * Created on Dec 7, 2003 by sviglas
 *
 * Modified on Dec 22, 2008 by sviglas
 *
 * This is part of the attica project.  Any subsequent modification
 * of the file should retain this disclaimer.
 * 
 * University of Edinburgh, School of Informatics
 */






/**
 * RelationIOManager: The basic class that undertakes relation I/O.
 *
 * @author sviglas
 */
class RelationIOManager {
	
    /** The relation of this manager. */
    private Relation relation;
	
    /** This manager's storage manager. */
    private StorageManager sm;
	
    /** The filename for the relation. */
    private String filename;
	
    /**
     * Constructs a new relation I/O manager.
     * 
     * @param sm this manager's storage manager.
     * @param relation the relation this manager handles I/O for.
     * @param filename the name of the file this relation is stored
     * in.
     */
    public RelationIOManager(StorageManager sm,
                             Relation relation, 
                             String filename) {
        this.sm = sm;
        this.relation = relation;
        this.filename = filename;
    } // RelationIOManager()
	
    /**
     * Inserts a new tuple into this relation.
     * 
     * @param tuple the tuple to be inserted.
     * @throws StorageManagerException thrown whenever there is an I/O
     * error.
     */
    public void insertTuple(Tuple tuple) throws StorageManagerException {
        insertTuple(tuple, true);
    } // insertTuple()

    
    /**
     * Inserts a new tuple specified as a list of comparable values
     * into this relation.
     *
     * @param values the list of comparables to be inserted.
     * @throws StorageManagerException thrown whenever there is an I/O
     * error.
     */
    public void insertTuple (List<Comparable> values)
        throws StorageManagerException {
        
        insertTuple(new Tuple(new TupleIdentifier(null, 0), values), true);
    } // insertTuple()

    
    /**
     * Inserts a new tuple into this relation.
     * 
     * @param tuple the tuple to be inserted.
     * @param newID re-assigns the tuple id if set to <pre>true</pre>.
     * @throws StorageManagerException thrown whenever there is an I/O
     * error.
     */
    public void insertTuple(Tuple tuple, boolean newID) 
	throws StorageManagerException {

        // to be honest, going over the code I have no idea why we may
        // not be re-assigning the tuple id, but I guess I was
        // thinking of something back then.
        try {
            // read in the last page of the file
            int pageNum = FileUtil.getNumberOfPages(getFileName());
            pageNum = (pageNum == 0) ? 0 : pageNum-1;
            PageIdentifier pid = 
                new PageIdentifier(getFileName(), pageNum);
            Page page = sm.readPage(relation, pid);
            int num = 0;
            if (page.getNumberOfTuples() != 0) {
                Tuple t = page.retrieveTuple(page.getNumberOfTuples()-1);
                num = t.getTupleIdentifier().getNumber()+1;
            }

            if (newID)
                tuple.setTupleIdentifier(new TupleIdentifier(getFileName(),
                                                             num));
            
            //long tn = page.getNumberOfTuples();
            if (! page.hasRoom(tuple)) {
                page = new Page(relation, new PageIdentifier(getFileName(),
                                                             pageNum+1));
                FileUtil.setNumberOfPages(getFileName(), pageNum+2);
            }
            page.addTuple(tuple);
            sm.writePage(page);
        }
        catch (Exception e) {
            e.printStackTrace(System.err);
            throw new StorageManagerException("I/O Error while inserting tuple "
                                              + "to file: " + getFileName()
                                              + " (" + e.getMessage() + ")", e);
        }
    } // insertTuple ()


    /**
     * Wrapper for castAttributes() with a list of comparables as the
     * parameter. See the notes there. (Package visible, because this
     * is embarrassing.)
     *
     * @param crap the crap needed by the other cast attributes
     * method.
     * @throws StorageManagerException thrown whenever the cast is not
     * possible.
     */
    void castAttributes(Tuple crap) throws StorageManagerException {
        castAttributes(crap.getValues());
    }

    /**
     * Package-visible (due to it being embarrassing) method to cast
     * comparables to correct comparable type. Confused? Yeah, you
     * should be.
     *
     * Assumes all comparables are strings and casts them to the
     * correct comparable type. I can't even begin to describe how
     * stupid this is. Actually, I can begin to describe how stupid it
     * is, but I won't finish on time. So, there.
     *
     * No, really. This is stupid. We're talking about a language that
     * after ensuring type-safety at compile-time, it cannot guarantee
     * it at run-time after serialization. This is just insane.
     *
     * @param crap the list of crap you feed the tuple.
     * @throws StorageManagerException when crap smells too bad.
     */
    void castAttributes(List<Comparable> crap) throws StorageManagerException {
        
        for (int i = 0; i < crap.size(); i++) {
            Comparable c = crap.get(i);

            Class<? extends Comparable> type =
                relation.getAttribute(i).getType();            
            if (type.equals(Byte.class))
                crap.set(i, new Byte((String) c));
            else if (type.equals(Short.class))
                crap.set(i, new Short((String) c));
            else if (type.equals(Integer.class))
                crap.set(i, new Integer((String) c));
            else if (type.equals(Long.class))
                crap.set(i, new Long((String) c));
            else if (type.equals(Float.class))
                crap.set(i, new Float((String) c));
            else if (type.equals(Double.class))
                crap.set(i, new Double((String) c));
            else if (type.equals(Character.class))
                crap.set(i, new Character(((String) c).charAt(0)));
            else if (! type.equals(String.class))
                throw new StorageManagerException("Unsupported type: "
                                                  + type + ".");
        }
    }
	
    /**
     * The name of the file for this manager.
     * 
     * @return the name of the file of this manager
     */
    public String getFileName () {
        return filename;
    } // getFileName()


    /**
     * Opens a page iterator over this relation.
     *
     * @return a page iterator over this relation.
     * @throws IOException whenever the iterator cannot be
     * instantiated from disk.
     * @throws StorageManagerException whenever the iterator cannot be
     * created after the file has been loaded.
     */
    public Iterable<Page> pages()
        throws IOException, StorageManagerException {
        
        return new PageIteratorWrapper();
    } // pages()

    /**
     * Opens a tuple iterator over this relation.
     *
     * @return a tuple iterator over this relation.
     * @throws IOException whenever the iterator cannot be
     * instantiated from disk.
     * @throws StorageManagerException whenever the iterator cannot be
     * created after the file has been loaded.
     */
    public Iterable<Tuple> tuples()
        throws IOException, StorageManagerException {
        
        return new TupleIteratorWrapper();
    } // tuples()

    
    /**
     * The basic iterator over pages of this relation.
     */
    class PageIteratorWrapper implements Iterable<Page> {
        /** The current page of the iterator. */
        private Page currentPage;

        /** The number of pages in the relation. */
        private int numPages;

        /** The current page offset. */
        private int pageOffset;

        /**
         * Constructs a new page iterator.
         */
        public PageIteratorWrapper()
            throws IOException, StorageManagerException {

            pageOffset = 0;
            numPages = FileUtil.getNumberOfPages(getFileName());
        } // PageIteratorWrapper()

        /**
         * Returns an iterator over pages.
         *
         * @return the iterator over pages.
         */
        public Iterator<Page> iterator() {
            return new Iterator<Page>() {
                public boolean hasNext() {
                    return pageOffset < numPages;
                } // hasNext()
                public Page next() throws NoSuchElementException {
                    try {
                        currentPage =
                            sm.readPage(relation,
                                        new PageIdentifier(getFileName(),
                                                           pageOffset++));
                        return currentPage;
                    }
                    catch (StorageManagerException sme) {
                        throw new NoSuchElementException("Could not read "
                                                         + "page to advance "
                                                         + "the iterator.");
                                                         
                    }
                } // next()
                public void remove() throws UnsupportedOperationException {
                    throw new UnsupportedOperationException("Cannot remove "
                                                            + "from page "
                                                            + "iterator.");
                } // remove()
            }; // new Iterator
        } // iterator()
    } // PageIteratorWrapper


    /**
     * The basic iterator over tuples of this relation.
     */
    class TupleIteratorWrapper implements Iterable<Tuple> {
        /** The page iterator. */
        private Iterator<Page> pages;
        
        /** The single-page tuple iterator. */
        private Iterator<Tuple> tuples;

        /** Keeps track of whether there are more elements to return. */
        private boolean more;

        /**
         * Constructs a new tuple iterator.
         */
        public TupleIteratorWrapper()
            throws IOException, StorageManagerException {
            
            pages = pages().iterator();
            more = pages.hasNext();
            tuples = null;
        } // TupleIterator()

        /**
         * Checks whether there are more tuples in this iterator.
         *
         * @return <code>true</code> if there are more tuples,
         * <code>false</code> otherwise.
         */
        public Iterator<Tuple> iterator() {
            return new Iterator<Tuple>() {
                public boolean hasNext() {
                    return more;
                } // hasNext()
                public Tuple next() throws NoSuchElementException {
                    if (tuples == null && more)
                        tuples = pages.next().iterator();

                    Tuple tuple = tuples.next();
                    if (tuples.hasNext()) more = true;
                    else if (pages.hasNext()) {
                        tuples = pages.next().iterator();
                        more = true;
                    }
                    else more = false;
                    return tuple;
                } // next()
                public void remove() throws UnsupportedOperationException {
                    throw new UnsupportedOperationException("Cannot remove "
                                                            + "from tuple "
                                                            + "iterator.");
                } // remove()
            }; // new Iterator
        } // iterator()
    } // TupleIteratorWrapper

    
    /**
     * Debug main.
     */
    public static void main (String [] args) {
        try {
            BufferManager bm = new BufferManager(100);
            StorageManager sm = new StorageManager(null, bm);
            
            List<Attribute> attributes = new ArrayList<Attribute>();
            attributes.add(new Attribute("integer", Integer.class));
            attributes.add(new Attribute("string", String.class));
            Relation relation = new Relation(attributes);
            String filename = args[0];
            sm.createFile(filename);
            RelationIOManager manager = 
                new RelationIOManager(sm, relation, filename);
            
            for (int i = 0; i < 30; i++) {
                List<Comparable> v = new ArrayList<Comparable>();
                v.add(new Integer(i));
                v.add(new String("bla"));
                Tuple tuple = new Tuple(new TupleIdentifier(filename, i), v);
                System.out.println("inserting: " + tuple);
                manager.insertTuple(tuple);
            }
            
            System.out.println("Tuples successfully inserted.");
            System.out.println("Opening tuple cursor...");

            for (Tuple tuple : manager.tuples())
                System.out.println("read: " + tuple);
            
        }
        catch (Exception e) {
            System.err.println("Exception: " + e.getMessage());
            e.printStackTrace(System.err);
        }	
    } // main()
    
} // RelationIOManager
/*
 * Created on Oct 9, 2003 by sviglas
 *
 * This is part of the attica project.  Any subsequent modification
 * of the file should retain this disclaimer.
 * 
 * University of Edinburgh, School of Informatics
 */

/**
 * Sizes: Set the various sizes for the attica storage engine.
 *
 * @author sviglas
 */
class Sizes {
    
    /** The number of bytes per attica page. */
    public static int PAGE_SIZE = 4096;
	

} // Sizes
/*
 * Created on Dec 10, 2003 by sviglas
 *
 * Modified on Dec 18, 2008 by sviglas
 *
 * This is part of the attica project.  Any subsequent modification
 * of the file should retain this disclaimer.
 * 
 * University of Edinburgh, School of Informatics
 */



/**
 * StorageManager: The storage manager of attica.  Provides low-level
 * page I/O to the rest of the system.
 *
 * @author sviglas
 */
class StorageManager {

    /** The storage manager's catalog. */
    private Catalog catalog;
	
    /** The storage manager's buffer manager. */
    private BufferManager buffer;

    /**
     * Initializes a new storage manager, given a catalog and a buffer
     * pool.
     * 
     * @param catalog this storage mananger's catalog.
     * @param buffer the buffer pool.
     */
    public StorageManager(Catalog catalog, BufferManager buffer) {
        this.catalog = catalog;
        this.buffer = buffer;
    } // StorageManager()


    /**
     * Registers a page with the buffer pool.
     *
     * @param page the page to be registered.
     * @throws StorageManagerException if the page cannot be registered.
     */
    public void registerPage(Page page) throws StorageManagerException {
        //buffer.putPage(page);
        writePage(page);
    } // registerPage()

    
    /**
     * Writes a page to disk.
     * 
     * @param page the page to be written
     * @throws StorageManagerException thrown whenever there is an I/O
     * error.
     */
    public synchronized void writePage(Page page) 
	throws StorageManagerException {
        
        try {
            // put the page in the buffer pool and check whether a
            // page has been evicted -- if it has, flush it            
            Page evictedPage = buffer.putPage(page, false);
            if (evictedPage != null) {
                String fn = evictedPage.getPageIdentifier().getFileName();
                DatabaseFile dbf = new DatabaseFile(fn,
                                                    DatabaseFile.READ_WRITE);
                PageIOManager.writePage(dbf, evictedPage);
                dbf.close();
            }
        }
        catch (Exception e) {
            throw new StorageManagerException("Error writing page to disk.", e);
        }
    } // writePage()

    
    /**
     * Reads a page from the database given the page's identifier.
     * 
     * @param pageid the identifier of the page to be read.
     * @return the page read.
     * @throws StorageManagerException whenever something goes wrong with
     * reading the page.
     */
    public synchronized Page readPage(Relation relation, 
                                      PageIdentifier pageid) 
	throws StorageManagerException {
        
        try {
            // if the buffer has the page read it from there
            if (buffer.containsPage(pageid)) {
                return buffer.getPage(pageid);
            }
            // otherwise read it from file and put it in the buffer pool
            else {
                String fn = pageid.getFileName();
                DatabaseFile dbf =
                    new DatabaseFile(fn, DatabaseFile.READ_WRITE);
                Page page = PageIOManager.readPage(relation, pageid, dbf);
                dbf.close();
                Page evictedPage = buffer.putPage(page, true);
                if (evictedPage != null) {
                    fn = evictedPage.getPageIdentifier().getFileName();
                    dbf = new DatabaseFile(fn, DatabaseFile.READ_WRITE);
                    PageIOManager.writePage(dbf, evictedPage);
                    dbf.close();
                }
                return page;
            }
        }
        catch (Exception e) {
            throw new StorageManagerException("Error reading page from "
                                              + "disk.", e);
        }
    } // readPage()

    
    /**
     * Given a relation, it creates a file name for it.
     * 
     * @param relation the relation for which a file name is to be created.
     * @return the filename associated with the relation.
     */
    public static String createFileName(Relation relation) {
        return new String(relation + "_" + relation.hashCode());
    } // createFileName()


    /**
     * Creates a new table in the database.
     * 
     * @param table the new table.
     * @throws StorageManagerException thrown whenever the table
     * cannot be created.
     */
    public void createTable(Table table) throws StorageManagerException {
        
        try {
            CatalogEntry entry = new CatalogEntry(table);
            catalog.createNewEntry(entry);
            createFile(catalog.getTableFileName(table.getName()));
        }
        catch (IllegalArgumentException iae) {
            throw new StorageManagerException("A table by the name "
                                              + table.getName()
                                              + " already exists", iae);
        }
        catch (NoSuchElementException nsee) {
            throw new StorageManagerException("Could not retrieve the table " +
                                              "that was just created", nsee);
        }
    } // createTable()
    
    
    /**
     * Deletes the specified table from the database.
     * 
     * @param tablename the name of the table to be deleted.
     * @throws NoSuchElementException thrown if the table does not
     * exist.
     * @throws StorageManagerException thrown if the file cannot be
     * deleted.
     */
    public void deleteTable(String tablename) 
	throws NoSuchElementException, StorageManagerException {
        deleteFile(catalog.getTableFileName(tablename));
        catalog.deleteTable(tablename);
    } // deleteTable()


    /**
     * Create a new file by the given file name.
     * 
     * @param filename the name of the new file.
     * @throws StorageManagerException thrown whenever the new file
     * cannot be deleted.
     */
    public void createFile(String filename) throws StorageManagerException {
        
        try {
            
            File file = new File(filename);
            if (! file.createNewFile()) {
                file.delete();
                file.createNewFile();
            } 
        }
        catch (Exception e) {
            throw new StorageManagerException("Could not create file "
                                              + filename + ".", e);
        }
    } // createFile()

    
    /**
     * Delete the specified file.
     * 
     * @param filename the name of the file to be deleted.
     * @throws StorageManagerException thrown whenever the file cannot be
     * deleted.
     */
    public void deleteFile(String filename) throws StorageManagerException {
        
        try {
            // invalidate the file's pages from the buffer manager
            buffer.invalidate(filename);
            File file = new File(filename);
            file.delete();
        }
        catch (Exception e) {
            throw new StorageManagerException("Could not delete file "
                                              + filename + ".", e);
        }
    } // deleteFile()    


    /**
     * Casts the list of comparables to the correct types before
     * inserting the tuple.
     *
     * @param tablename the target table.
     * @param tuple the new tuple.
     * @throws NoSuchElementException thrown whenever the specified table
     * does not exist.
     * @throws StorageManagerException thrown whenever ther insertion is
     * not possible.
     */
    public void castAndInsertTuple(String tablename, Tuple tuple)
        throws NoSuchElementException, StorageManagerException {

        Table table = catalog.getTable(tablename);
        String file = catalog.getTableFileName(tablename);
        TableIOManager man = new TableIOManager(this, table, file);
        man.castAttributes(tuple);
        man.insertTuple(tuple);
    } // castAndInsertTuple()
    
    
    /**
     * Inserts a new tuple into the given table.
     * 
     * @param tablename the name of the table where the tuple is to 
     * be inserted.
     * @param tuple the new tuple.
     * @throws NoSuchElementException thrown whenever the specified table
     * does not exist.
     * @throws StorageManagerException thrown whenever ther insertion is
     * not possible.
     */
    public void insertTuple(String tablename, Tuple tuple)
	throws NoSuchElementException, StorageManagerException {
        
        Table table = catalog.getTable(tablename);
        String file = catalog.getTableFileName(tablename);
        TableIOManager man = new TableIOManager(this, table, file);
        man.insertTuple(tuple);
    } // insertTuple()


    /**
     * Casts the list of comparables to the correct types before
     * inserting the tuple.
     *
     * @param tablename the target table.
     * @param values the values to be inserted.
     * @throws NoSuchElementException thrown whenever the specified table
     * does not exist.
     * @throws StorageManagerException thrown whenever ther insertion is
     * not possible.
     */
    public void castAndInsertTuple(String tablename,
                                   List<Comparable> values)
        throws NoSuchElementException, StorageManagerException {

        Table table = catalog.getTable(tablename);
        String file = catalog.getTableFileName(tablename);
        TableIOManager man = new TableIOManager(this, table, file);
        man.castAttributes(values);
        man.insertTuple(values);
    } // castAndInsertTuple()
    
    
    /**
     * Inserts a new tuple into the given table.
     * 
     * @param tablename the name of the table where the tuple is to 
     * be inserted.
     * @param values the values of the tuple.
     * @throws NoSuchElementException thrown whenever the specified table
     * does not exist.
     * @throws StorageManagerException thrown whenever ther insertion is
     * not possible.
     */
    public void insertTuple(String tablename,
                            List<Comparable> values) 
	throws NoSuchElementException, StorageManagerException {
        
        Table table = catalog.getTable(tablename);
        String file = catalog.getTableFileName(tablename);
        TableIOManager man = new TableIOManager(this, table, file);
        man.insertTuple(values);
    } // insertTuple()

    
    public void shutdown() throws StorageManagerException {

        try {
            for (Page page : buffer.pages()) {
                // this loop is probably the worst thing I've ever
                // written. there should be a per-file dump through a
                // registry of open files and yada-yada-yada... but
                // it's too late in the game for such a rewrite
                String fn = page.getPageIdentifier().getFileName();
                DatabaseFile dbf = new DatabaseFile(fn,
                                                    DatabaseFile.READ_WRITE);
                PageIOManager.writePage(dbf, page);
                dbf.close();
            }
        }
        catch (Exception e) {
            throw new StorageManagerException("Could not properly shut down "
                                              + "the storage manager: "
                                              + e.getMessage(), e);
        }
    } // shutdown()

    /**
     * Returns the number of buffer pool pages.
     *
     * @return the number of buffer pool pages.
     */
    public int getNumberOfBufferPoolPages() {
        return buffer.getNumberOfPages();
    } // getNumberOfBufferPoolPages()
    
    /**
     * Debug main().
     * 
     * @param args
     */
    public static void main (String args[]) {
        try {
            String filename = args[0];
            
            List<Attribute> attrs = new ArrayList<Attribute>();
            attrs.add(new Attribute("character", Character.class));
            attrs.add(new Attribute("byte", Byte.class));
            attrs.add(new Attribute("short", Short.class));
            attrs.add(new Attribute("integer", Integer.class));
            attrs.add(new Attribute("long", Long.class));
            attrs.add(new Attribute("float", Float.class));
            attrs.add(new Attribute("double", Double.class));
            attrs.add(new Attribute("string", String.class));
            Relation rel = new Relation(attrs);
            
            List<Comparable> v = new ArrayList<Comparable>();
            v.add(new Character('a'));
            v.add(new Byte((byte) 26));
            v.add(new Short((short) 312));
            v.add(new Integer(2048));
            v.add(new Long(34567));
            v.add(new Float(12.3));
            v.add(new Double(25.6));
            v.add(new String("bla bla"));
            
            PageIdentifier pid1 = new PageIdentifier(filename, 0);
            Page p1 = new Page(rel, pid1);
            for (int i = 0; i < 20; i++) {
                Tuple t = new Tuple(new TupleIdentifier(filename, i), v);
                p1.addTuple(t);
            }
            System.out.println(p1);
            PageIdentifier pid2 = new PageIdentifier(filename, 1);
            Page p2 = new Page(rel, pid2);
            for (int i = 0; i < 30; i++) {
                Tuple t = new Tuple(new TupleIdentifier(filename, i), v);
                p2.addTuple(t);
            }
            System.out.println(p2);
            
            System.out.println("Writing pages");
            DatabaseFile dfb = new DatabaseFile(filename, 
                                                DatabaseFile.READ_WRITE);
            
            StorageManager sm = new StorageManager(null, 
                                                   new BufferManager(100));
            sm.writePage(p1);
            sm.writePage(p2);
            
            System.out.println("Reading pages");
            p1 = sm.readPage(rel, p1.getPageIdentifier());
            p2 = sm.readPage(rel, p2.getPageIdentifier());
            
            System.out.println(p1);
            System.out.println(p2);
        }
        catch (Exception e) {
            System.err.println("Exception e: " + e.getMessage());
            e.printStackTrace(System.err);
        }
    } // main()
} // StorageManager
/*
 * Created on Oct 10, 2003 by sviglas
 *
 * Modified on Dec 18, 2008 by sviglas
 *
 * This is part of the attica project.  Any subsequent modification
 * of the file should retain this disclaimer.
 * 
 * University of Edinburgh, School of Informatics
 */

/**
 * StorageManagerException: The base class for all the exceptions of
 * the storage manager.
 *
 * @author sviglas
 */
class StorageManagerException extends Exception {
	
    /**
     * Creates a new exception to be thrown.
     * 
     * @param message the message of the exeption.
     */
    public StorageManagerException (String message) {
        super(message);
    } // StorageManagerException()


    /**
     * Creates a new exception to be thrown.
     * 
     * @param m the message of the exeption.
     * @param e the originating throwable.
     */
    public StorageManagerException(String m, Throwable e) {
        super(m, e);
    } // StorageManagerException()
} // StorageManagerException
/*
 * Created on Dec 7, 2003 by sviglas
 *
 * This is part of the attica project.  Any subsequent modification
 * of the file should retain this disclaimer.
 * 
 * University of Edinburgh, School of Informatics
 */


/**
 * TableIOManager: Convenience class to extend RelationIOManager for
 * tables.
 *
 * @author sviglas
 */
class TableIOManager extends RelationIOManager {
	
    /**
     * Constructs a new table manager.
     * 
     * @param sm the storage manager for this table manager.
     * @param table the table this manager manages.
     * @param filename the name of the file the table is stored
     * in.
     */
    public TableIOManager(StorageManager sm, Table table, String filename) {
        super(sm, table, filename);
    } // TableIOManager()
    
} // TableIOManager
/*
 * Created on Oct 5, 2003 by sviglas
 *
 * This is part of the attica project.  Any subsequent modification
 * of the file should retain this disclaimer.
 * 
 * University of Edinburgh, School of Informatics
 */


/**
 * Tuple: The basic encapsulation of an attica <code>Tuple</code>.
 *
 * @author sviglas
 */
class Tuple {
	
    /** The identifier of this tuple. */
    private TupleIdentifier tupleIdentifier;
    
    /** The values stored in the tuple. */
    private List<Comparable> values;
	
    /**
     * Constructs a new empty <code>Tuple</code>.
     */
    public Tuple() {
        this.tupleIdentifier = null;
        this.values = new ArrayList<Comparable>();
    } // Tuple()

    
    /**
     * Constructs a new tuple given an identifier and a 
     * <code>List</code> of values.
     * 
     * @param tupleIdentifier the identifier of the tuple.
     * @param values the values to be added.
     */
    public Tuple(TupleIdentifier tupleIdentifier, List<Comparable> values) {
        this.tupleIdentifier = tupleIdentifier;
        this.values = values;
        // may have to copy things if there are any problems during
        // updates, but we'll see till then; a reference will do for now
        // this.values = new ArrayList<Comparable>(values);
    } // Tuple()

    
    /**
     * Is this tuple intermediate or not?
     * 
     * @return <code>true</code> if the tuple is intermediate,
     * <code>false</code> otherwise.
     */
    public boolean isIntermediate() {
        // brain-damaged. no, really. brain-damaged.
        return (getTupleIdentifier() instanceof IntermediateTupleIdentifier);
    } // isIntermediate()

    
    /**
     * Returns the identifier of this tuple.
     * 
     * @return this tuple's identifier.
     */
    public TupleIdentifier getTupleIdentifier() {
        return tupleIdentifier;
    } // getTupleIdentifier()


    /**
     * Sets this tuple's identifier.
     *
     * @param td the tuple's identifier.
     */
    public void setTupleIdentifier(TupleIdentifier td) {
        tupleIdentifier = td;
    } // setTupleIdentifier()

    
    /**
     * Sets a value of the tuple. *NOTE* it does not perform any type 
     * or bounds checking.
     * 
     * @param slot the slot of the tuple accessed.
     * @param value the new value of the tuple.
     */
    public void setValue(int slot, Comparable value) {
        values.set(slot, value);
    } // setValue()

    
    /**
     * Sets the values of this tuple.
     * 
     * @param values the vector of new values.
     */
    public void setValues(List<Comparable> values) {        
        this.values.clear();
        this.values = values;
    } // setValues()


    /**
     * Returns the size in slots of this tuple.
     * 
     * @return the number of slots in this tuple.
     */
    public int size() {
        return values.size();
    } // size()

    
    /**
     * Returns the values of this tuple.
     * 
     * @return this tuple's values.
     */
    public List<Comparable> getValues() {
        return values;
    } // getValues()
    
	
    /**
     * Returns the slot of the tuple cast to a primitive character.
     * 
     * @param slot the slot of the <code>Tuple</code> accessed.
     * @return the specified slot of the tuple cast to a primitive
     * character.
     * @throws ClassCastException if the cast fails.
     */
    public char asChar(int slot) throws ClassCastException {
        Character c  = (Character) values.get(slot);
        return c.charValue();
    } // asChar()

    
    /**
     * Returns the slot of the tuple cast to a primitive byte.
     * 
     * @param slot the slot of the <code>Tuple</code> accessed.
     * @return the specified slot of the tuple cast to a primitive
     * byte.
     * @throws ClassCastException if the cast fails.
     */
    public byte asByte(int slot) throws ClassCastException {
        Byte b  = (Byte) values.get(slot);
        return b.byteValue();
    } // asByte()

    
    /**
     * Returns the slot of the tuple cast to a primitive short.
     * 
     * @param slot the slot of the <code>Tuple</code> accessed.
     * @return the specified slot of the tuple cast to a primitive
     * short.
     * @throws ClassCastException if the cast fails.
     */
    public short asShort(int slot) throws ClassCastException {
        Short s  = (Short) values.get(slot);
        return s.shortValue();
    } // asShort()
    
    
    /**
     * Returns the slot of the tuple cast to a primitive integer.
     * 
     * @param slot the slot of the <code>Tuple</code> accessed.
     * @return the specified slot of the tuple cast to a primitive
     * integer.
     * @throws ClassCastException if the cast fails.
     */
    public int asInt(int slot) throws ClassCastException {
        Integer in = (Integer) values.get(slot);
        return in.intValue();
    } // asInt()

    
    /**
     * Returns the slot of the tuple cast to a primitive long.
     * 
     * @param slot the slot of the <code>Tuple</code> accessed.
     * @return the specified slot of the tuple cast to a primitive
     * long.
     * @throws ClassCastException if the cast fails.
     */
    public long asLong(int slot) throws ClassCastException {
        Long l = (Long) values.get(slot);
        return l.longValue();
    } // asLong()
	

    /**
     * Returns the slot of the tuple cast to a primitive float.
     * 
     * @param slot the slot of the <code>Tuple</code> accessed.
     * @return the specified slot of the tuple cast to a primitive
     * float.
     * @throws ClassCastException if the cast fails.
     */
    public float asFloat(int slot) throws ClassCastException {
        Float f = (Float) values.get(slot);
        return f.floatValue();
    } // asFloat() 

    
    /**
     * Returns the slot of the tuple cast to a primitive double.
     * 
     * @param slot the slot of the <code>Tuple</code> accessed.
     * @return the specified slot of the tuple cast to a primitive
     * double.
     * @throws ClassCastException if the cast fails.
     */
    public double asDouble(int slot) throws ClassCastException {
        Double doub = (Double) values.get(slot);
        return doub.doubleValue();
    } // asDouble() 
	

    /**
     * Returns the slot of the tuple cast to a Java <code>String</code>.
     * 
     * @param slot the slot of the <code>Tuple</code> accessed.
     * @return the specified slot of the tuple cast to a Java
     * <code>String</code>.
     * @throws ClassCastException if the cast fails.
     */
    public String asString(int slot) throws ClassCastException {
        String str = (String) values.get(slot);
        return str;
    } // asString()

    
    /**
     * Returns the specified slot of this tuple as a generic
     * Comparable.
     * 
     * @param slot ths slot of the <code>Tuple</code> accessed.
     * @return the specified slot of the tuple as a generic Comparable.
     */
    public Comparable getValue(int slot) {
        return values.get(slot);
    } // getValue()


    /**
     * Tests this tuple to an object for equality.
     *
     * @param o the object to compare this tuple to.
     * @return <code>true</code> if this tuple is equal to
     * <code>o</code>, <code>false</code> otherwise.
     */
    @Override
    public boolean equals(Object o) {
        
        if (o == this) return true;
        if (! (o instanceof Tuple)) return false;
        Tuple t = (Tuple) o;
        if (size() != t.size()) return false;
        if (! getTupleIdentifier().equals(t.getTupleIdentifier())) return false;
        int i = 0;
        for (Comparable comp : values) {
            if (! comp.equals(t.getValue(i))) return false;
            i++;
        }
        return true;
    }

    /**
     * Returns a hash code for this tuple.
     *
     * @return a hash code for this tuple.
     */
    @Override
    public int hashCode() {
        int hash = 17;
        hash += 31*hash + getTupleIdentifier().hashCode();
        for (Comparable comp : values)
            hash += 31*hash + comp.hashCode();
        return hash;
    }
    
    /**
     * Textual representation.
     */
    @Override
    public String toString() {
        return tupleIdentifier.toString() + " : " + values.toString();
    } // toString()

    public String toStringFormatted() {
        return "(" + tupleIdentifier.getNumber() + ") : " + values.toString();
    }
} // Tuple
/*
 * Created on Dec 7, 2003 by sviglas
 *
 * This is part of the attica project.  Any subsequent modification
 * of the file should retain this disclaimer.
 * 
 * University of Edinburgh, School of Informatics
 */

/**
 * TupleIdentifier: An identifier for an attica tuple.
 *
 * @author sviglas
 */
class TupleIdentifier {
	
    /** The filename this tuple belongs to. */
    private String filename;
	
    /** The number of this tuple. */
    private int number;

    
    /**
     * Constructs a new tuple identifier given just the name of the file
     * the tuple belongs to -- the number will be set later.
     * 
     * @param filename the name of the file this tuple belongs to.
     */
    public TupleIdentifier(String filename) {
        this(filename, -1);
    } // TupleIdentifier()

    
    /**
     * Constructs a new tuple given the filename it belongs to and its
     * number in that file.
     * 
     * @param filename the filename this tuple belongs to.
     * @param number the number of this tuple.
     */
    public TupleIdentifier(String filename, int number) {
        this.filename = filename;
        this.number = number;
    } // TupleIdentifier()

    
    /**
     * Returns the name of the file this tuple belongs to.
     * 
     * @return the name of the file this tuple belongs to.
     */
    public String getFileName() {
        return filename;
    } // getFileName()

    
    /**
     * Returns the number of this tuple in the file.
     * 
     * @return the number of this tuple in the file.
     */
    public int getNumber() {
        return number;
    } // getNumber()

    
    /**
     * Sets the number of this tuple in the file.
     * 
     * @param number the new number of this tuple in the file.
     */
    public void setNumber(int number){
        this.number = number;
    } // setNumber()

    
    /**
     * Checks two tuple identifiers for equality.
     * 
     * @param o an object to compare this identifier to.
     * @return <pre>true</pre> if the two tuple identifiers are equal
     * <pre>false</pre> otherwise.
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;

        if (! (o instanceof TupleIdentifier)) return false;
        TupleIdentifier tid = (TupleIdentifier) o;
        return getFileName().equals(tid.getFileName()) &&
            getNumber() == tid.getNumber();
    } // equals()

    /**
     * Computes the hashcode of this tuple identifier.
     *
     * @return this tuple identifier's hashcode.
     */
    @Override
    public int hashCode() {
        int hash = 17;
        hash = hash*31 + getFileName().hashCode();
        return hash*31 + getNumber();
    } // hashCode()

    /**
     * Textual representation.
     */
    @Override
    public String toString () {
        return "[" + (getFileName() != null ? getFileName() : "")
            + " - " + getNumber() + "]";
    } // toString()

} // TupleIdentifier
/*
 * Created on Dec 2, 2003 by sviglas
 *
 * Modified on Dec 23, 2003 by sviglas
 *
 * This is part of the attica project.  Any subsequent modification
 * of the file should retain this disclaimer.
 * 
 * University of Edinburgh, School of Informatics
 */





/**
 * TupleIOManager: Takes care of tuple input and output.
 *
 * @author sviglas
 */
class TupleIOManager {
	
    /** The relation tuples belong to. */
    private Relation relation;
	
    /** The filename of this relation. */
    private String filename;
	
    /**
     * Construct a new manager given a relation schema.
     * 
     * @param relation the relation schema.
     * @param filename the name of the relation's file.
     */
    public TupleIOManager(Relation relation, String filename) { 
        this.relation = relation;
        this.filename = filename;
    } // TupleIOManager()

    
    /**
     * Writes a tuple to a byte array -- should be used when there is
     * page buffering.
     * 
     * @param tuple the tuple to be written.
     * @param bytes the byte array the tuple should be written to.
     * @param start the starting offset into the byte array.
     * @return the new offset in the byte array.
     * @throws StorageManagerException thrown whenever there is an I/O
     * error.
     */
    public int writeTuple(Tuple tuple, byte [] bytes, int start) 
	throws StorageManagerException {
        
        // write the tuple id
        byte [] b = Convert.toByte(tuple.getTupleIdentifier().getNumber());
        System.arraycopy(b, 0, bytes, start, b.length);
        start += b.length;
        // make an attempt to have the array GC'ed (since Java is
        // brain-damaged and we can't release memory -- what the F
        // were you people thinking?
        //
        // but it's OK, I'm not going to rant about an array of four
        // bytes now; there are plenty more reasons to bash Java for.
        b = null;
        
        // NB slot++ is in the dumpSlot() call -- uberweiss
        int slot = 0;    
        for (Attribute attr : relation)
            start = dumpSlot(attr.getType(), tuple, slot++, bytes, start);

        return start;
    } // writeTuple()

    
    /**
     * Reads a tuple from a byte array -- should be used when there is
     * page buffering.
     * 
     * @param bytes the byte array.
     * @param start the starting offset into the byte array.
     * @return a pair consisting of the tuple read and the new offset
     * in the byte array.
     * @throws StorageManagerException thrown whenever there is an I/O
     * error.
     */
    public Pair<Tuple, Integer> readTuple(byte [] bytes, int start)
        throws StorageManagerException {
        
        // read in the tuple id
        byte [] b = new byte[Convert.INT_SIZE];
        System.arraycopy(bytes, start, b, 0, b.length);
        int id = Convert.toInt(b);
        start += b.length;
        List<Comparable> values = new ArrayList<Comparable>();
        int slot = 0;
        for (Iterator<Attribute> it = relation.iterator();
             it.hasNext(); slot++) {
            Pair<? extends Comparable, Integer> pair =
                fetch(it.next().getType(), bytes, start);
            start = pair.second;
            values.add(pair.first);
        }
        Tuple t = new Tuple(new TupleIdentifier(filename, id), values);
        return new Pair<Tuple, Integer>(t, new Integer(start));
    } // readTuple()

    
    /**
     * Low-level method to dump a tuple slot to a byte array.
     * 
     * @param type the type of the slot.
     * @param t the tuple.
     * @param s the slot of the tuple to be dumped.
     * @param bytes the byte array where the slot will be dumped.
     * @param start the starting offset.
     * @return the new offset in the array.
     * @throws StorageManagerException thrown whenever there is an I/O
     * error.
     */
    protected int dumpSlot(Class<? extends Comparable> type, Tuple t,
                           int s, byte [] bytes, int start) 
	throws StorageManagerException {

        if (type.equals(Character.class)) {
            byte [] b = Convert.toByte(t.asChar(s));
            System.arraycopy(b, 0, bytes, start, b.length);
            return start + b.length;
        }
        else if (type.equals(Byte.class)) {
            bytes[start] = t.asByte(s);
            return start+1;
        }
        else if (type.equals(Short.class)) {
            byte [] b = Convert.toByte(t.asShort(s));
            System.arraycopy(b, 0, bytes, start, b.length);
            return start + b.length;
        }
        else if (type.equals(Integer.class)) {
            byte [] b = Convert.toByte(t.asInt(s));
            System.arraycopy(b, 0, bytes, start, b.length);
            return start + b.length;
        }
        else if (type.equals(Long.class)) {
            byte [] b = Convert.toByte(t.asLong(s));
            System.arraycopy(b, 0, bytes, start, b.length);
            return start + b.length;
        }
        else if (type.equals(Float.class)) {
            byte [] b = Convert.toByte(t.asFloat(s));
            System.arraycopy(b, 0, bytes, start, b.length);
            return start + b.length;
        }
        else if (type.equals(Double.class)) {
            byte [] b = Convert.toByte(t.asDouble(s));
            System.arraycopy(b, 0, bytes, start, b.length);
            return start + b.length;
        }
        else if (type.equals(String.class)) {
            String st = t.asString(s);
            int len = st.length();
            byte [] b = Convert.toByte(len);
            System.arraycopy(b, 0, bytes, start, b.length);
            start += b.length;
            b = Convert.toByte(st);
            System.arraycopy(b, 0, bytes, start, b.length);
            return start + b.length;
        }
        else {
            // this is an unsupported type for the time being, so
            // throw an exception
            throw new StorageManagerException("Unsupported type when "
                                              + "writing tuple: "
                                              + type.getClass().getName()
                                              + ".");
        }
    } // dumpSlot()
    
    
    /**
     * Low-level method to read a slot from a byte array.
     * 
     * @param type the type of the slot to be read.
     * @param bytes the input byte array.
     * @param start the starting offset in the byte array.
     * @return a pair of (value, offset) with the value read and the
     * new offset in the byte array.
     * @throws StorageManagerException thrown whenever there is an I/O
     * error.
     */
    protected Pair<? extends Comparable, Integer>
        fetch(Class<? extends Comparable> type, byte [] bytes, int start)
	throws StorageManagerException {
        
        try {
            if (type.equals(Character.class)) {
                byte [] b = new byte[Convert.CHAR_SIZE];
                System.arraycopy(bytes, start, b, 0, b.length);
                return new Pair<Character, Integer>(
                    new Character(Convert.toChar(b)), 
                    start + b.length);
            }
            else if (type.equals(Byte.class)) {
                return new Pair<Byte, Integer>(bytes[start], 
                                               start + 1);
            }
            else if (type.equals(Short.class)) {
                byte [] b = new byte[Convert.SHORT_SIZE];
                System.arraycopy(bytes, start, b, 0, b.length);
                return new Pair<Short, Integer>(new Short(Convert.toShort(b)),
                                                start + b.length);
            }
            else if (type.equals(Integer.class)) {
                byte [] b = new byte[Convert.INT_SIZE];
                System.arraycopy(bytes, start, b, 0, b.length);
                return new Pair<Integer, Integer>(new Integer(Convert.toInt(b)),
                                                  start + b.length);
            }
            else if (type.equals(Long.class)) {
                byte [] b = new byte[Convert.LONG_SIZE];
                System.arraycopy(bytes, start, b, 0, b.length);
                return new Pair<Long, Integer>(new Long(Convert.toLong(b)),
                                               start + b.length);
            }
            else if (type.equals(Float.class)) {
                byte [] b = new byte[Convert.FLOAT_SIZE];
                System.arraycopy(bytes, start, b, 0, b.length);
                return new Pair<Float, Integer>(new Float(Convert.toFloat(b)),
                                                start + b.length);
            }
            else if (type.equals(Double.class)) {
                byte [] b = new byte[Convert.DOUBLE_SIZE];
                System.arraycopy(bytes, start, b, 0, b.length);
                return new Pair<Double, Integer>(
                    new Double(Convert.toDouble(b)),
                    start + b.length);
            }
            else if (type.equals(String.class)) {
                byte [] b = new byte[Convert.INT_SIZE];
                System.arraycopy(bytes, start, b, 0, b.length);
                start += b.length;
                int stLength = Convert.toInt(b);
                b = new byte[2*stLength];
                System.arraycopy(bytes, start, b, 0, b.length);
                String str = Convert.toString(b);
                return new Pair<String, Integer>(str, start + b.length);
            }
            else {
                throw new StorageManagerException("Unsupported type: "
                                                  + type.getClass().getName()
                                                  + ".");
            }
        }
        catch (ArrayIndexOutOfBoundsException aiob) {
            throw new StorageManagerException("Generic error while reading " +
                                              "table row (boundary error.)",
                                              aiob);
        }
    } // fetch()


    /**
     * Calculates the byte size of a tuple of this IO manager.
     *
     * @param t the tuple.
     * @return the byte size of the tuple.
     */
    public int byteSize(Tuple t) {
        return byteSize(relation, t);
    }
    
    /**
     * Calculates the byte size of a tuple given its relation. (Lame,
     * lame, lame... I should hang my head in shame. Or go play a
     * game. Or see if I can start a big fire with a small flame. Or
     * stop looking for words that rhyme with lame.)     
     *
     * @param rel the relation.
     * @param t the tuple.
     * @return the size in bytes of tuple.
     */
    public static int byteSize(Relation rel, Tuple t) {
        // one long for the id
        int size = Convert.INT_SIZE;
	int slot = 0;
        for (Attribute it : rel) {
            Class<?> type = it.getType();
            if (type.equals(Character.class)) size += Convert.CHAR_SIZE;
            else if (type.equals(Byte.class)) size += 1;
            else if (type.equals(Short.class)) size += Convert.SHORT_SIZE;
            else if (type.equals(Integer.class)) size += Convert.INT_SIZE;
            else if (type.equals(Long.class)) size += Convert.LONG_SIZE;
            else if (type.equals(Float.class)) size += Convert.FLOAT_SIZE;
            else if (type.equals(Double.class)) size += Convert.DOUBLE_SIZE;
            else if (type.equals(String.class))
                size += Convert.INT_SIZE + 2 * t.asString(slot).length();

            slot++;
        }
	
        return size;
    } // byteSize()

    
    /**
     * Debug main.
     */
    /*
    public static void main (String args []) {
        try {
            List<Attribute> attrs = new ArrayList<Attribute>();
            attrs.add(new Attribute("character", Character.class));
            attrs.add(new Attribute("byte", Byte.class));
            attrs.add(new Attribute("short", Short.class));
            attrs.add(new Attribute("integer", Integer.class));
            attrs.add(new Attribute("long", Long.class));
            attrs.add(new Attribute("float", Float.class));
	    attrs.add(new Attribute("double", Double.class));
            attrs.add(new Attribute("string", String.class));
            Relation rel = new Relation(attrs);
		
            List<Comparable> v = new ArrayList<Comparable>();
            v.add(new Character('a'));
            v.add(new Byte((byte) 26));
            v.add(new Short((short) 312));
            v.add(new Integer(2048));
            v.add(new Long(34567));
            v.add(new Float(12.3));
            v.add(new Double(25.6));
            v.add(new String("bla bla"));
            Tuple t = new Tuple(new TupleIdentifier(args[0], 0), v);
            TupleIOManager man = new TupleIOManager(rel, args[0]);
            
            java.io.RandomAccessFile raf = new java.io.RandomAccessFile(args[0], "rw");
            
            System.out.println("writing tuple...");
            man.writeTuple(t, raf);
            raf.close();
            
            raf = new java.io.RandomAccessFile(args[0], "r");
            System.out.println("reading tuple...");
            t = man.readTuple(raf);
            System.out.println(t);
            raf.close();
        }
        catch (Exception e) {
            System.err.println("Exception: " + e.getMessage());
            e.printStackTrace(System.err);
        }
    } // main()
    */
    
} // TupleIOManager

/**
 * Convenience methods for parsing command-line arguments.
 *
 * @author sviglas
 */

class Args {

    /**
     * Return true of false depending on whether the given argument
     * is present or not.
     *
     * @param args the arguments.
     * @param trig the argument to be looked for.
     * @return true of the argument is there, false otherwise.
     */
    public static boolean gettrig(String [] args, String trig) {
        for (int i = 0; i < args.length; i++)
            if (args[i].equals(trig)) return true;
        
        return false;
    } // gettrig()
    
    /**
     * Given command line arguments as an array of strings, get the value
     * for an optional argument, null if it's not there.
     *
     * @param args the arguments.
     * @param opt the optional argument to be retrieved.
     * @return the value of the optional argument, or <code>null</code>
     * if the argument is not present.
     */
    public static String getopt(String [] args, String opt) {
        for (int i = 0; i < args.length; i++)
            if (args[i].equals(opt))
                return args[i+1];
        
        return null;
    } // getopt()
	
    /**
     * Given command line arguments as an array of strings, get the value
     * for an optional argument, and a default value if it is not there.
     *
     * @param args the arguments.
     * @param opt the optional argument to be retrieved.
     * @param def the default value.
     * @return the value of the optional argument, or the default value
     * if the argument is not present.
     */
    public static String getopt(String [] args, String opt, String def) {
        String val = getopt(args, opt);
        return (val == null ? def : val);
    } // getopt()
    
} // Args
/*
 * Created on Dec 4, 2003 by org.dejave.glas
 *
 * This is part of the attica project.  Any subsequenct modification
 * of the file should retain this disclaimer.
 * 
 * University of Edinburgh, School of Informatics
 */

/**
 * @author org.dejave.glas
 *
 * Convert: Conversions between primitive types and byte arrays
 */
class Convert {

    public static final int INT_SIZE = 4;
    public static final int LONG_SIZE = 8;
    public static final int SHORT_SIZE = 2;
    public static final int CHAR_SIZE = 2;
    public static final int FLOAT_SIZE = 4;
    public static final int DOUBLE_SIZE = 8;
    
    public Convert() {}

    public static final void toByte(int i, byte [] bytes, int o) {
        for (byte b = 0; b <= 3; b++)
            bytes[o+b] = (byte) (i >>> (3 - b)*8);
    }
    
    public static final byte [] toByte(int i) {
        byte b[] = new byte[4];
        toByte(i, b, 0);
        return b;
    }

    public static final void toByte(short w, byte [] bytes, int o) {
        for (byte b = 0; b <= 1; b++)
            bytes[o+b] = (byte) (w >>> (1 - b)*8);
    }

    public static final byte [] toByte(short w) {
        byte b[] = new byte[2];
        toByte(w, b, 0);
        return b;
    } // toByte()

    public static final void toByte(long l, byte [] bytes, int o) {
        for (byte b = 0; b <= 7; b++)
            bytes[o+b] = (byte)(int) (l >>> (7 - b)*8);
    }

    public static final byte [] toByte (long l) {
        byte b[] = new byte[8];
        toByte(l, b, 0);
        return b;
    } // toByte()

    public static final void toByte(char c, byte [] bytes, int o) {
        for (byte b = 0; b <= 1; b++)
            bytes[o+b] = (byte) (c >>> (1-b)*8);
    }

    public static final byte [] toByte (char c) {
        byte b[] = new byte[2];
        toByte(c, b, 0);
        return b;
    }

    public static void toByte(float f, byte [] bytes, int o) {
        int i = Float.floatToIntBits(f);
        toByte(i, bytes, o);
    }

    public static final byte [] toByte (float f) {  
        byte b[] = new byte[4];
        toByte(f, b, 0);
        return b;
    }

    public static void toByte(double d, byte [] bytes, int o) {
        long l = Double.doubleToLongBits(d);
        toByte(l, bytes, o);
    }

    public static final byte [] toByte (double d) {
        byte [] b = new byte[8];
        toByte(d, b, 0);
        return b;
    }

    public static final byte [] toByte(String s) {
        byte [] b = new byte[2*s.length()];
        toByte(s, b, 0);
        return b;
    }

    public static final byte [] toByte(String s, byte [] bytes, int o) {
        int length = s.length();
        for (int i = 0; i < length; i++) {
            byte [] two = toByte(s.charAt(i));
            bytes[2*i] = two[0];
            bytes[2*i+1] = two[1];
        }
        return bytes;
    }

    public static final int toInt (byte b[]) {
        return toInt(b, 0);
    }

    public static final int toInt (byte b[], int o) {
        int i = 0;
        for (int b0 = 0; b0 <= 3; b0++) {
            int j;
            if (b[o+b0] < 0) {
                j = (byte) (b[o+b0] & 0x7f);
                j |= 0x80;
            }
            else {
                j = b[o+b0];
            }
            i |= j;
            if (b0 < 3) i <<= 8;
        }
        return i;
    }

    public static final short toShort(byte b[]) {
        return toShort(b, 0);
    }
    
    public static final short toShort(byte b[], int o) {
        short word0 = 0;
        for (int b0 = 0; b0 <= 1; b0++) {
            short word1;
            if (b[o+b0] < 0) {
                word1 = (byte)(b[o+b0] & 0x7f);
                word1 |= 0x80;
            }
            else {
                word1 = b[o+b0];
            }
            word0 |= word1;
            if(b0 < 1) word0 <<= 8;
        }
        return word0;
    }
    
    public static final long toLong (byte b[]) {
        return toLong(b, 0);
    }

    public static final long toLong (byte b[], int o) {
        long l = 0L;
        for (int b0 = 0; b0 <= 7; b0++) {
            long l1;
            if (b[o+b0] < 0) {
                l1 = (byte)(b[o+b0] & 0x7f);
                l1 |= 128L;
            }
            else {
                l1 = b[o+b0];
            }
            l |= l1;
            if(b0 < 7) l <<= 8;
        }
        return l;
    }
    
    public static final char toChar (byte b[]) {
        return toChar(b, 0);
    }
    
    public static final char toChar (byte b[], int o) {
        char c = 0;
        c = (char)((c | (char)b[o]) << 8);
        c |= (char)b[o+1];
        return c;
    }
    
    public static final float toFloat (byte b[]) {
        return toFloat(b, 0);
    }
    
    public static final float toFloat (byte b[], int o) {
        float f = 0.0F;
        Float float1 = new Float(f);
        int i = toInt(b, o);
        f = Float.intBitsToFloat(i);
        return f;
    }
    
    
    public static final double toDouble (byte b[]) {
        return toDouble(b, 0);
    }
    
    public static final double toDouble (byte b[], int o) {
        double d = 0.0D;
        Double double1 = new Double(d);
        long l = toLong(b, o);
        d = Double.longBitsToDouble(l);
        return d;
    }

    public static final String toString(byte [] b) {
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < b.length-1; i = i+2) {
            byte [] two = new byte[2];
            two[0] = b[i];
            two[1] = b[i+1];
            sb.append(toChar(two));
        }
        return sb.toString();
    }
    
    public static void main (String args[]) {
    
        byte b[] = new byte[Convert.LONG_SIZE*2560];
        for (int i = 0; i < 2560; i++)
            Convert.toByte((long) i, b, i*Convert.LONG_SIZE);
            
        for (int i = 0; i < 2560; i++)
            System.out.println(Convert.toLong(b, i*Convert.LONG_SIZE));
            
        for (int i = 0; i < 2560; i++)
            System.out.println(Convert.toLong(b, i*Convert.LONG_SIZE));
    
        /*
        byte b[] = new byte[8];
        //b = Convert.toByte(0x47851f79);
        Convert.toByte(129, b, 0);
        System.out.println("int: " + 0x47851f79);
        for (int i = 0; i <= 3; i++)
            System.out.print("   " + b[i]);
        
        System.out.println();
        System.out.println("back to int: " + Convert.toInt(b));
        System.out.println();
        b = Convert.toByte((short)-177);
        System.out.println("short: " + -177);
        for (int j = 0; j <= 1; j++)
            System.out.print("   " + b[j]);
        
        System.out.println();
        System.out.println("back to short: " + Convert.toShort(b));
        System.out.println();
        b = Convert.toByte(0x48a749338441e818L);
        System.out.println("long: " + 0x48a749338441e818L);
        for (int k = 0; k <= 7; k++)
            System.out.print("   " + b[k]);
        
        System.out.println();
        System.out.println("back to long: " + Convert.toLong(b));
        System.out.println();
        b = Convert.toByte('k');
        System.out.println("char: " + 'k');
        for (int l = 0; l <= 1; l++)
            System.out.print("   " + b[l]);
        
        System.out.println();
        System.out.println("back to char: " + Convert.toChar(b));
        System.out.println();
        b = Convert.toByte(-564351.4F);
        System.out.println("float: " + -564351.4F);
        for (int i1 = 0; i1 <= 3; i1++)
            System.out.print("   " + b[i1]);
        
        System.out.println();
        System.out.println("back to float: " + Convert.toFloat(b));
        System.out.println();
        b = Convert.toByte(139245812345123.45D);
        System.out.println("double: " + 139245812345123.45D);
        for (int j1 = 0; j1 <= 7; j1++)
            System.out.print("   " + b[j1]);
        
        System.out.println();
        System.out.println("back to double: " + Convert.toDouble(b));
        */
    }
} // Convert //:~


class DuplicateHashMap <K extends Comparable<? super K>, V> {
    private Map<K, List<V>> map;

    public DuplicateHashMap() {
	map = new HashMap<K, List<V> >();
    }

    public void put(K k, V v) {
	List<V> list = map.get(k);
	if (list == null) list = new LinkedList<V>();
	list.add(v);
	map.put(k, list);
    }

    public int size() {
        int sz = 0;
        for (List<V> list : map.values())
            sz += list.size();
        return sz;
    }

    public List<V> get(K k) {
        List<V> res = map.get(k);
        if (res == null) return Collections.emptyList();
        else return res;
    }

    public List<V> remove(K k) { return map.remove(k); }

    public boolean containsKey(K k) { return get(k) != null; }

    @Override
    public String toString() { return map.toString(); }

    public void clear() { map.clear(); }

}
				       



/**
 * Does the boilerplate work for loggers.
 *
 * @author Stratis Viglas &lt;org.dejave.glas@inf.ed.ac.uk&gt;
 * @version $0.1$
 */
public final class Logger {
    
    public static java.util.logging.Logger createLogger(String n,
                                                        Handler h,
                                                        Formatter f) {
        h.setFormatter(f);
        return createLogger(n, h);
    }
    
    public static java.util.logging.Logger createLogger(String n, Handler r) {
        java.util.logging.Logger logger = 
            java.util.logging.Logger.getLogger(n);
        Handler [] handlers = logger.getHandlers();
        for (int i = 0; i < handlers.length; i++)
            logger.removeHandler(handlers[i]);
        logger.setUseParentHandlers(false);
        logger.setLevel(Level.INFO);
        logger.addHandler(r);
        return logger;
    }
    
}


class LRUMap <K, V> {

    public static final int DEFAULT_CAPACITY = 1000;

    private int capacity;
    private Map<K, V> map;

    public LRUMap() { this(DEFAULT_CAPACITY); }

    public LRUMap(int cap) {
        capacity = cap;
        float factor = 0.75f;
        int mapCapacity = (int) Math.ceil(capacity / factor) + 1;
        map = new LinkedHashMap<K, V>(mapCapacity, factor, true) {
            protected boolean removeEldestEntry(Map.Entry<K, V> entry) {
                boolean ret = size() > LRUMap.this.capacity;
                if (ret) LRUMap.this.onRemove(entry);
                return ret;
            }
        };
    }

    public void put(K k, V v) { map.put(k, v); }
    public V get(K k) { return map.get(k); }
    public int size() { return map.size(); }
    public void clear() { map.clear(); }
    public boolean containsKey(K k) { return map.containsKey(k); }    
    public boolean containsValue(V v) { return map.containsValue(v); }
    public V remove(K k) { return map.remove(k); }
    public Set<Map.Entry<K, V> > entrySet() { return map.entrySet(); }
    protected void onRemove(Map.Entry<K, V> entry) {}
    
    public String toString() { return map.toString(); }
    
    public static void main (String [] args) {
        LRUMap<Integer, String> map = new LRUMap<Integer, String>(3);
        System.out.println("will insert one");
        map.put(1, "one");
        System.out.println("one inserted, will insert two");
        map.put(2, "two");
        System.out.println("two inserted, will insert three");
        map.put(3, "three");
        System.out.println("three inserted, will insert four");
        System.out.println(map.toString());
        map.get(1);
        System.out.println(map.toString());
        map.put(4, "four");
        System.out.println("four inserted");
        System.out.println(map.toString());
    }
}

/**
 * @author sviglas
 *
 * Utility class that wraps a pair of values.
 */

class Pair <F, S> {
    /** The first element. */
    public F first;
    /** The second element. */
    public S second;


    /**
     * Default constructor.
     */
    public Pair() {
        first = null;
        second = null;
    }

    /**
     * Constructs a new pair given its first and second elements.
     *
     * @param f the first element.
     * @param s the second element.
     */
    public Pair(F f, S s) {
        first = f;
        second = s;
    } // Pair()


    /**
     * Tests this pair for equality to an object.
     *
     * @param o the object to test for equality with.
     * @return <code>true</code> if <code>this</code> is equal to
     * <code>o</code>, <code>false</code> otherwise.
     */
    @Override
    @SuppressWarnings("unchecked")
    public boolean equals(Object o) {
        if (o == this) return true;
        if (! (o instanceof Pair)) return false;
        try {
            Pair<F, S> p = (Pair<F, S>) o;
            return (first == null
                    ? p.first == null : first.equals(p.first))
                && (second == null
                    ? p.second == null : second.equals(p.second));
        }
        catch (ClassCastException cce) {
            return false;
        }
    } // equals()


    /**
     * Computes a hashcode for this pair.
     *
     * @return this pair's hashcode.
     */
    @Override
    public int hashCode() {
        int hash = 17;
        int code = (first != null ? first.hashCode() : 0);
        hash = hash*31 + code;
        code = (second != null ? second.hashCode() : 0);
        return hash*31 + code;
    } // hashCode()

    /**
     * Textual representation.
     *
     * @return this pair's textual representation.
     */
    @Override
    public String toString() {
        return "(" + (first != null ? first : ("null"))
            + ", " + (second != null ? second : ("null")) + ")";
    } // toString()
}

class Triplet <F, S, T> {
    public F first;
    public S second;
    public T third;

    public Triplet() { this(null, null, null); }
    
    public Triplet(F f, S s, T t) {
        first = f;
        second = s;
	third = t;
    }
    
    public String toString() {
	return "(" + first + ", " + second + ", " + third + ")";
    }
}
