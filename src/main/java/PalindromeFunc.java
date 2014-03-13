/**
 * Created by kim on 3/13/14.
 */


import org.openrdf.query.algebra.evaluation.function.Function;
import org.openrdf.query.algebra.evaluation.ValueExprEvaluationException;
import org.openrdf.model.impl.*;
import org.openrdf.model.*;

/**
 * a custom SPARQL function that determines whether an input literal string is
 * a palindrome.
 */
public class PalindromeFunc implements Function {

    // define a constant for the namespace of our custom function
    public static final String NAMESPACE = "http://example.org/custom-function/";

    /**
     * return the URI 'http://example.org/custom-function/palindrome' as a String
     */
    public String getURI() {
        return NAMESPACE + "palindrome";
    }

    /**
     * Executes the palindrome function.
     *
     * @return A boolean literal representing true if the input argument is a palindrome,
     *         false otherwise.
     *
     * @throws ValueExprEvaluationException
     *         if more than one argument is supplied or if the supplied argument is
     *         not a literal.
     */
    public Value evaluate(ValueFactory valueFactory, Value... args)
            throws ValueExprEvaluationException
    {
        // our palindrome function expects only a single argument, so throw an error
        // if there's more than one
        if (args.length != 1) {
            throw new ValueExprEvaluationException("palindrome function requires" +
                    "exactly 1 argument, got " + args.length);
        }

        Value arg = args[0];

        // check if the argument is a literal, if not, we throw an error
        if (arg instanceof Literal) {
            // get the actual string value that we want to check for palindrome-ness.
            String label = ((Literal)arg).getLabel();

            // we invert our string
            String inverted = "";
            for (int i = label.length() - 1; i >= 0; i--) {
                inverted += label.charAt(i);
            }

            // a string is a palindrome if it is equal to its own inverse
            boolean palindrome = inverted.equalsIgnoreCase(label);

            // a function is always expected to return a Value object, so we
            // return our boolean result as a Literal
            return valueFactory.createLiteral(palindrome);
        }
        else {
            throw new ValueExprEvaluationException(
                    "invalid argument (literal expected): " + arg);
        }
    }
}