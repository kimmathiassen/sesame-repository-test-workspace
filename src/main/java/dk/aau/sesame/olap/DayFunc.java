package dk.aau.sesame.olap;

import org.openrdf.query.algebra.evaluation.function.datetime.Day;


/**
 * Created by alex on 4/14/14.
 */
public class DayFunc extends Day
{
    public static final String NAMESPACE = "http://example.org/customfunction/";
    public String getURI()
    {
        return NAMESPACE + "day";
    }

}
