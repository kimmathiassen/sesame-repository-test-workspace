package dk.aau.sesame.olap;

import org.openrdf.model.Literal;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.datatypes.XMLDatatypeUtil;
import org.openrdf.query.algebra.evaluation.ValueExprEvaluationException;
import org.openrdf.query.algebra.evaluation.function.Function;
import org.openrdf.query.algebra.evaluation.function.datetime.Year;

import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.Duration;
import javax.xml.datatype.XMLGregorianCalendar;

/**
 * Created by alex on 4/14/14.
 */
public class YearFunc extends Year
{
    public static final String NAMESPACE = "http://example.org/customfunction/";
    public String getURI()
    {
        return NAMESPACE + "year";
    }

}
