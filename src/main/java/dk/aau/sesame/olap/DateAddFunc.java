package dk.aau.sesame.olap;

import org.openrdf.model.Literal;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.datatypes.XMLDatatypeUtil;
import org.openrdf.model.vocabulary.FN;
import org.openrdf.query.algebra.evaluation.ValueExprEvaluationException;
import org.openrdf.query.algebra.evaluation.function.Function;

import javax.xml.datatype.*;

/**
 * Created by alex on 4/14/14.
 */
public class DateAddFunc implements Function
{
    public static final String NAMESPACE = "http://example.org/customfunction/";
    public String getURI()
    {
        return NAMESPACE + "dateadd";
    }

    public Literal evaluate(ValueFactory valueFactory, Value... args)
            throws ValueExprEvaluationException
    {
        try
        {
        if (args.length != 3) {
            throw new ValueExprEvaluationException("dateadd requires 3 argument, got " + args.length);
        }

        Value durationTypeValue = args[0];
        Value durationValue = args[1];
        Value dateValue = args[2];
        if (durationTypeValue instanceof Literal) {
            Literal durationTypeLiteral = (Literal)durationTypeValue;
            if (durationValue instanceof Literal) {
                Literal durationLiteral = (Literal)durationValue;
                if (dateValue instanceof Literal) {
                    Literal dateLiteral = (Literal)dateValue;
                    if (XMLDatatypeUtil.isCalendarDatatype(dateLiteral.getDatatype()) ||
                            XMLDatatypeUtil.isValidDate(dateLiteral.stringValue()) ||
                            XMLDatatypeUtil.isValidDateTime(dateLiteral.stringValue())) {
                        if (XMLDatatypeUtil.isValidInt(durationLiteral.stringValue())) {
                            try {
                                XMLGregorianCalendar calValue = dateLiteral.calendarValue();
                                DatatypeFactory factory = DatatypeFactory.newInstance();

                                String durationTypeStr = durationTypeLiteral.stringValue().toLowerCase();
                                String XMLDateString;
                                String durationStr;
                                long durationLong = durationLiteral.longValue();
                                if(durationLong >= 0)
                                {
                                    XMLDateString = "P";
                                    durationStr = String.valueOf(durationLong);
                                }
                                else
                                {
                                    XMLDateString = "-P";
                                    durationStr = String.valueOf(-durationLong);
                                }
                                if(durationTypeStr.equals("year")) {
                                    XMLDateString += durationStr + "Y";
                                }
                                else if(durationTypeStr.equals("month")) {
                                    XMLDateString += durationStr + "M";
                                }
                                else if(durationTypeStr.equals("day")) {
                                    XMLDateString += durationStr + "D";
                                }
                                else {
                                    throw new ValueExprEvaluationException("unexpected input value for function: " + args[0]);
                                }
                                Duration duration = factory.newDuration(XMLDateString);
                                calValue.add(duration);
                                return valueFactory.createLiteral(calValue);
                            }
                            catch (IllegalArgumentException e) {
                                throw new ValueExprEvaluationException("illegal calendar value: " + args[2]);
                            } catch (DatatypeConfigurationException e) {
                                throw new RuntimeException("unable to instantiate DatatypeFactory, used to create the resulting date. Implementation missing (http://docs.oracle.com/javase/7/docs/api/javax/xml/datatype/DatatypeFactory.html#newInstance())");
                            }
                        }
                        else
                        {
                            throw new ValueExprEvaluationException("unexpected input value for function: " + args[0]);
                        }
                    }
                    else
                    {
                        throw new ValueExprEvaluationException("unexpected input value for function: " + args[2]);
                    }
                }
                else
                {
                    throw new ValueExprEvaluationException("unexpected input value for function: " + args[2]);
                }
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
