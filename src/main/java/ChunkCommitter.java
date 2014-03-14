import org.openrdf.model.Statement;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.util.RDFInserter;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;

/**
 * Created by alex on 3/13/14.
 */
public class ChunkCommitter implements RDFHandler
{
    private RDFInserter inserter;
    private RepositoryConnection conn;
    private long start;
    private boolean commit;

    private long count = 0L;

    // do intermittent commit every 1.000.000 triples
    private long chunksize = 1000000L;

    public ChunkCommitter(RepositoryConnection conn, boolean commit) {
        inserter = new RDFInserter(conn);
        this.conn = conn;
        this.commit = commit;
        start = System.nanoTime();
    }

    @Override
    public void startRDF() throws RDFHandlerException {
        inserter.startRDF();
    }

    @Override
    public void endRDF() throws RDFHandlerException {
        inserter.endRDF();
    }

    @Override
    public void handleNamespace(String prefix, String uri)
            throws RDFHandlerException {
        inserter.handleNamespace(prefix, uri);
    }

    @Override
    public void handleStatement(Statement st) throws RDFHandlerException {
        inserter.handleStatement(st);
        count++;
        // do an intermittent commit whenever the number of triples
        // has reached a multiple of the chunk size
        if (count % chunksize == 0) {
            try {
                if(commit)
                {
                    conn.commit();
                }
                System.out.println("Triples loaded: " + count + ". Time: " + (System.nanoTime()-start)/1000000 + " ms");
            } catch (RepositoryException e) {
                throw new RDFHandlerException(e);
            }
        }
    }

    @Override
    public void handleComment(String comment) throws RDFHandlerException {
        inserter.handleComment(comment);
    }
}
