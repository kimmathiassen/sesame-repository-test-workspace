/**
 * Created by kim on 2/20/14.
 */
import info.aduna.iteration.Iterations;
import org.openrdf.model.*;
import org.openrdf.model.impl.LinkedHashModel;
import org.openrdf.model.vocabulary.FOAF;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.model.vocabulary.XMLSchema;
import org.openrdf.repository.*;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.rio.RDFParseException;
import org.openrdf.sail.memory.MemoryStore;
import org.openrdf.OpenRDFException;
import org.openrdf.repository.Repository;
import org.openrdf.rio.RDFFormat;
import java.io.File;
import java.io.FileInputStream;
import java.net.URL;
import org.openrdf.repository.Repository;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.sail.nativerdf.NativeStore;
import java.io.IOException;
import org.openrdf.rio.Rio;
import java.util.List;
import org.openrdf.OpenRDFException;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryLanguage;

public class main {

    private static File dataDir = new File("dbs/test/");
    private static String indexes = "spoc,posc,cosp";
    public static void main(String args[]) throws RepositoryException {
        System.out.println("Hello World!");
        org.openrdf.repository.Repository repo = null;
        loadTestData();
        repo = new SailRepository(new NativeStore(dataDir, indexes));

        repo.initialize();

        RepositoryConnection con = repo.getConnection();
        try
        {
            String queryString = "SELECT ?x ?y WHERE { ?x ?p ?y } ";
            TupleQuery tupleQuery = con.prepareTupleQuery(QueryLanguage.SPARQL, queryString);

            TupleQueryResult result = tupleQuery.evaluate();

            List<String> bindingNames = result.getBindingNames();
            int i = 0;
            while (result.hasNext()) {
                BindingSet bindingSet = result.next();
                Value firstValue = bindingSet.getValue(bindingNames.get(0));
                Value secondValue = bindingSet.getValue(bindingNames.get(1));

                System.out.println(firstValue + " " + secondValue);
                ++i;
            }
            System.out.println(i);
        }
        catch (OpenRDFException e) {
            e.printStackTrace();
        }
        finally {
            con.close();
        }
    }

    private static void loadTestData() throws RepositoryException {
        File file = new File("test.ttl");
        org.openrdf.repository.Repository repo = new SailRepository(new NativeStore(dataDir, indexes));
        repo.initialize();
        RepositoryConnection con = repo.getConnection();
        try
        {
            con.add(file,"" , RDFFormat.TURTLE);
        }
        catch (RDFParseException e) {
            e.printStackTrace();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        finally {
            con.close();
            repo.shutDown();
        }
    }

}
