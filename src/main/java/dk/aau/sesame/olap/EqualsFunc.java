package dk.aau.sesame.olap;

import org.openrdf.model.*;
import org.openrdf.model.datatypes.XMLDatatypeUtil;
import org.openrdf.query.algebra.evaluation.ValueExprEvaluationException;
import org.openrdf.query.algebra.evaluation.function.Function;

import javax.xml.datatype.*;

/**
 * Created by alex on 4/14/14.
 */
public class EqualsFunc implements Function
{
    public static final String NAMESPACE = "http://example.org/customfunction/";
    public String getURI()
    {
        return NAMESPACE + "equals";
    }

    public Literal evaluate(ValueFactory valueFactory, Value... args)
            throws ValueExprEvaluationException
    {
        try
        {
            if (args.length != 2) {
                throw new ValueExprEvaluationException("equal requires 2 argument, got " + args.length);
            }

            Value firstVal = args[0];
            Value secondVal = args[1];
            if (firstVal instanceof Literal && secondVal instanceof Literal ||
                    firstVal instanceof Resource && secondVal instanceof Resource ||
                    firstVal instanceof URI && secondVal instanceof URI) {
                return valueFactory.createLiteral(firstVal.stringValue().equals(secondVal.stringValue()) ? 1 : 0);
            }
            else
            {
                throw new ValueExprEvaluationException("unmatching inputs for function: " + args[0] + " " + args[1]);
            }
        }
        catch (Exception e)
        {
            System.out.println(e);
            throw e;
        }
    }
}
