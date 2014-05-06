package dk.aau.sesame.olap;

import org.openrdf.query.algebra.evaluation.function.datetime.Month;


/**
 * Created by alex on 4/14/14.
 */
public class MonthFunc extends Month
{
    public static final String NAMESPACE = "http://example.org/customfunction/";
    public String getURI()
    {
        return NAMESPACE + "month";
    }

}
