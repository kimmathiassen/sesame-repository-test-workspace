package dk.aau.sesame.olap;

import org.openrdf.model.Statement;
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
    private String inputDataDir;

    public ConstructMode(boolean commit, String indexes) {
        this(commit, indexes,null);
    }


    public ConstructMode(boolean commit, String indexes, String inputDataDir) {
        this.commit = commit;
        this.indexes = indexes;
        this.inputDataDir = inputDataDir;
    }

    @Override
    public void handle(String dataDir, String arg) throws Exception
    {
        if(inputDataDir == null || inputDataDir.length() == 0)
        {
            inputDataDir = dataDir;
        }
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
        RepositoryConnection inputCon;
        org.openrdf.repository.Repository inputRepo = null;
        if(inputDataDir == dataDir)
        {
            inputCon = con;
        }
        else
        {
            inputRepo = new SailRepository(new NativeStore(new File(inputDataDir), indexes));
            inputRepo.initialize();
            inputCon = inputRepo.getConnection();
        }
        try
        {
            long start = System.nanoTime();
            con.begin();
            GraphQuery graphQuery = inputCon.prepareGraphQuery(QueryLanguage.SPARQL, arg);
            GraphQueryResult result = graphQuery.evaluate();
            while(result.hasNext())
            {
                Statement stm = result.next();
                System.out.println(stm);
                break;
            }
            con.add(result);
            con.commit();
            System.out.println("Query processed in time: " + (System.nanoTime()-start)/1000000 + " ms");
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            if(inputRepo != null)
            {
                inputCon.close();
                inputRepo.shutDown();
            }
            con.close();
            repo.shutDown();
        }
    }
}
