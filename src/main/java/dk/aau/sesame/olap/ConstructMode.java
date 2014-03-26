package dk.aau.sesame.olap;

import org.openrdf.query.*;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.sail.nativerdf.NativeStore;

import java.io.File;
import java.util.Scanner;

/**
 * Created by alex on 3/26/14.
 */
public class ConstructMode implements Mode {
    private boolean commit;
    private String indexes;

    public ConstructMode(boolean commit, String indexes) {
        this.commit = commit;
        this.indexes = indexes;
    }

    @Override
    public void handle(String dataDir, String arg) throws Exception
    {
        File queryFile = new File(arg);
        if(queryFile.exists())
        {
            if(queryFile.isDirectory())
            {
                System.out.println("Going into directory: " + queryFile.getName());
                for(File file : queryFile.listFiles())
                {
                    handle(dataDir, file.getAbsolutePath());
                }
            }
            else if(queryFile.canRead())
            {
                System.out.println("Reading query in: " + queryFile.getName());
                arg = new Scanner(queryFile).useDelimiter("\\Z").next();
                handle(dataDir, arg);
            }
            else
            {
                throw new IllegalArgumentException("Unable to read file: '" + arg + "' for query");
            }
            return;
        }

        org.openrdf.repository.Repository repo = new SailRepository(new NativeStore(new File(dataDir), indexes));
        repo.initialize();
        RepositoryConnection con = repo.getConnection();
        try
        {
            long start = System.nanoTime();
            con.begin();
            GraphQuery graphQuery = con.prepareGraphQuery(QueryLanguage.SPARQL, arg);
            GraphQueryResult result = graphQuery.evaluate();
            con.add(result);
            if(commit)
            {
                con.commit();
                con.begin();
            }
            con.commit();
            System.out.println("Query processed in time: " + (System.nanoTime()-start)/1000000 + " ms");
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            con.close();
            repo.shutDown();
        }
    }
}
