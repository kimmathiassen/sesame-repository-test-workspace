package dk.aau.sesame.olap;

import org.openrdf.model.Literal;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.datatypes.XMLDatatypeUtil;
import org.openrdf.query.algebra.evaluation.ValueExprEvaluationException;
import org.openrdf.query.algebra.evaluation.function.Function;

import javax.xml.datatype.*;

/**
 * Created by alex on 4/14/14.
 */
public class OrFunc implements Function
{
    public static final String NAMESPACE = "http://example.org/customfunction/";
    public String getURI()
    {
        return NAMESPACE + "or";
    }

    public Literal evaluate(ValueFactory valueFactory, Value... args)
            throws ValueExprEvaluationException
    {
        try
        {
            if (args.length != 2) {
                throw new ValueExprEvaluationException("or requires 2 argument, got " + args.length);
            }

            Value firstVal = args[0];
            Value secondVal = args[1];
            if (firstVal instanceof Literal) {
                Literal firstLiteral = (Literal)firstVal;
                if (secondVal instanceof Literal) {
                    Literal secondLiteral = (Literal)secondVal;
                    boolean first;
                    boolean second;
                    if (XMLDatatypeUtil.isValidInt(firstLiteral.stringValue())) {
                        first = Integer.parseInt(firstLiteral.stringValue()) != 0;
                    } else if(XMLDatatypeUtil.isValidBoolean(firstLiteral.stringValue())) {
                        first = Boolean.parseBoolean(firstLiteral.stringValue());
                    } else {
                        throw new ValueExprEvaluationException("unexpected input value for function: " + args[0]);
                    }
                    if (XMLDatatypeUtil.isValidInt(secondLiteral.stringValue())) {
                        second = Integer.parseInt(secondLiteral.stringValue()) != 0;
                    } else if(XMLDatatypeUtil.isValidBoolean(secondLiteral.stringValue())) {
                        second = Boolean.parseBoolean(secondLiteral.stringValue());
                    } else {
                        throw new ValueExprEvaluationException("unexpected input value for function: " + args[1]);
                    }
                    return valueFactory.createLiteral(first || second ? 1 : 0);
                }
                else
                {
                    throw new ValueExprEvaluationException("unexpected input value for function: " + args[1]);
                }
            }
            else
            {
                throw new ValueExprEvaluationException("unexpected input value for function: " + args[0]);
            }
        }
        catch (Exception e)
        {
            System.out.println(e);
            throw e;
        }
    }
}
