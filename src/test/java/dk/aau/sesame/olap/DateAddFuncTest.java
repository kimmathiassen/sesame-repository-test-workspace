package dk.aau.sesame.olap;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import com.sun.org.apache.xerces.internal.jaxp.datatype.XMLGregorianCalendarImpl;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.model.vocabulary.XMLSchema;
import org.openrdf.query.algebra.evaluation.ValueExprEvaluationException;

import javax.xml.datatype.XMLGregorianCalendar;
import java.util.Date;

/**
 * @author jeen
 */
public class DateAddFuncTest {

    private ValueFactory f = new ValueFactoryImpl();

    private DateAddFunc dateAdd;

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp()
            throws Exception
    {
        dateAdd = new DateAddFunc();
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown()
            throws Exception
    {
    }

    @Test
    public void testEvaluate1() {
        try {
            Literal result = dateAdd.evaluate(f, f.createLiteral("day"),f.createLiteral(1),
                    f.createLiteral("2011-01-10",new URIImpl("http://www.w3.org/2001/XMLSchema#date")));

            assertNotNull(result);
            assertEquals(XMLSchema.DATE, result.getDatatype());

            assertEquals("2011-01-11", result.getLabel());

        }
        catch (ValueExprEvaluationException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testEvaluate2() {
        try {
            Literal result = dateAdd.evaluate(f, f.createLiteral("day"),f.createLiteral(1),
                    f.createLiteral("2011-01-10T14:45:13",new URIImpl("http://www.w3.org/2001/XMLSchema#datetime")));

            assertNotNull(result);
            assertEquals(XMLSchema.DATETIME, result.getDatatype());

            assertEquals("2011-01-11T14:45:13", result.getLabel());

        }
        catch (ValueExprEvaluationException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testEvaluate3() {
        try {
            Literal result = dateAdd.evaluate(f, f.createLiteral("day"),f.createLiteral(1),
                    f.createLiteral("2011-01-10"));

            assertNotNull(result);
            assertEquals(XMLSchema.DATE, result.getDatatype());

            assertEquals("2011-01-11", result.getLabel());

        }
        catch (ValueExprEvaluationException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testEvaluate4() {
        try {
            Literal result = dateAdd.evaluate(f, f.createLiteral("day"),f.createLiteral(1),
                    f.createLiteral("2011-01-10T14:45:13"));

            assertNotNull(result);
            assertEquals(XMLSchema.DATETIME, result.getDatatype());

            assertEquals("2011-01-11T14:45:13", result.getLabel());

        }
        catch (ValueExprEvaluationException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testEvaluate5() {
        try {
            Literal result = dateAdd.evaluate(f, f.createLiteral("day"),f.createLiteral(-1),
                    f.createLiteral("2011-01-10T14:45:13"));

            assertNotNull(result);
            assertEquals(XMLSchema.DATETIME, result.getDatatype());

            assertEquals("2011-01-09T14:45:13", result.getLabel());

        }
        catch (ValueExprEvaluationException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testEvaluate6() {
        try {
            Literal result = dateAdd.evaluate(f, f.createLiteral("day"),f.createLiteral(0),
                    f.createLiteral("2011-01-10T14:45:13"));

            assertNotNull(result);
            assertEquals(XMLSchema.DATETIME, result.getDatatype());

            assertEquals("2011-01-10T14:45:13", result.getLabel());

        }
        catch (ValueExprEvaluationException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testEvaluate7() {
        try {
            Literal result = dateAdd.evaluate(f, f.createLiteral("year"),f.createLiteral(1),
                    f.createLiteral("2011-01-10T14:45:13"));

            assertNotNull(result);
            assertEquals(XMLSchema.DATETIME, result.getDatatype());

            assertEquals("2012-01-10T14:45:13", result.getLabel());

        }
        catch (ValueExprEvaluationException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

}
