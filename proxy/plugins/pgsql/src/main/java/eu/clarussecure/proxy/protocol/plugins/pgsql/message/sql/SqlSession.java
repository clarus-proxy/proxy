package eu.clarussecure.proxy.protocol.plugins.pgsql.message.sql;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import eu.clarussecure.dataoperations.Promise;
import eu.clarussecure.proxy.protocol.plugins.pgsql.message.PgsqlRowDescriptionMessage;
import eu.clarussecure.proxy.spi.CString;
import eu.clarussecure.proxy.spi.Operation;
import net.sf.jsqlparser.statement.Statement;

public class SqlSession {
    public static class BufferedStatement {
        private CString original;
        private final boolean toProcess;
        private Statement stmt;
        private CString modified;

        public BufferedStatement(CString statement) {
            this(statement, false);
        }

        public BufferedStatement(CString statement, boolean toProcess) {
            this.original = statement;
            this.toProcess = toProcess;
            this.modified = null;
        }

        public CString getOriginal() {
            return original;
        }

        public void releaseOriginal() {
            if (original != null && original.release()) {
                original = null;
            }
        }

        public boolean isToProcess() {
            return toProcess;
        }

        public Statement getStmt() {
            return stmt;
        }

        public void setStmt(Statement stmt) {
            this.stmt = stmt;
        }

        public CString getModified() {
            return modified;
        }

        public void setModified(CString modified) {
            this.modified = modified;
            // Release internal buffer of original statement
            releaseOriginal();
            stmt = null;
        }

        public void releaseModified() {
            if (modified != null && modified.release()) {
                modified = null;
            }
        }

        public CString getResult() {
            return toProcess ? modified : original;
        }
    }

    private CString databaseName;
    private boolean inTransaction;
    private boolean inDatasetCreation;
    private Operation currentOperation;
    private Promise promise;
    private TransferMode transferMode;
    private List<BufferedStatement> bufferedStatements;
    private List<PgsqlRowDescriptionMessage.Field> rowDescription;
    private int commandResultsToIgnore;
    private int readyForQueryToIgnore;

    public CString getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(CString databaseName) {
        if (this.databaseName != null) {
            // Release internal buffer of database name
            this.databaseName.release();
        }
        this.databaseName = databaseName;
    }

    public boolean isInTransaction() {
        return inTransaction;
    }

    public void setInTransaction(boolean inTransaction) {
        this.inTransaction = inTransaction;
    }

    public boolean isInDatasetCreation() {
        return inDatasetCreation;
    }

    public void setInDatasetCreation(boolean inDatasetCreation) {
        this.inDatasetCreation = inDatasetCreation;
    }

    public Operation getCurrentOperation() {
        return currentOperation;
    }

    public void setCurrentOperation(Operation operation) {
        this.currentOperation = operation;
    }

    public Promise getPromise() {
        return promise;
    }

    public void setPromise(Promise promise) {
        this.promise = promise;
    }

    public TransferMode getTransferMode() {
        return transferMode;
    }

    public void setTransferMode(TransferMode transferMode) {
        this.transferMode = transferMode;
    }

    public List<BufferedStatement> getBufferedStatements() {
        if (bufferedStatements == null) {
            bufferedStatements = Collections.synchronizedList(new ArrayList<>());
        }
        return bufferedStatements;
    }

    public void setBufferedStatements(List<BufferedStatement> bufferedStatements) {
        this.bufferedStatements = bufferedStatements;
    }

    public boolean addBufferedStatements(CString statement, boolean toProcess) {
        // Retain internal buffer of statement
        if (statement.isBuffered()) {
            statement.retain();
        }
        return getBufferedStatements().add(new BufferedStatement(statement, toProcess));
    }

    public void resetBufferedStatements() {
        if (bufferedStatements != null) {
            for (BufferedStatement bufferedStatement : bufferedStatements) {
                // Release internal buffer of original statement
                bufferedStatement.releaseOriginal();
                // Release internal buffer of modified statement
                bufferedStatement.releaseModified();
            }
            bufferedStatements.clear();
        }
    }

    public List<PgsqlRowDescriptionMessage.Field> getRowDescription() {
        return this.rowDescription;
    }

    public void setRowDescription(List<PgsqlRowDescriptionMessage.Field> rowDescription) {
        // Retain internal buffer of each field name
        if (rowDescription != null) {
            for (PgsqlRowDescriptionMessage.Field field : rowDescription) {
                if (field.getName().isBuffered()) {
                    field.getName().retain();
                }
            }
        }
        this.rowDescription = rowDescription;
    }

    public void resetRowDescription() {
        if (rowDescription != null) {
            for (PgsqlRowDescriptionMessage.Field field : rowDescription) {
                if (field.getName().release()) {
                    field.setName(null);
                }
            }
            rowDescription = null;
        }
    }

    public int getCommandResultsToIgnore() {
        return commandResultsToIgnore;
    }

    public void resetCommandResultsToIgnore() {
        commandResultsToIgnore = 0;
    }

    public void incrementeCommandResultsToIgnore() {
        commandResultsToIgnore ++;
    }

    public void decrementeCommandResultsToIgnore() {
        commandResultsToIgnore --;
    }

    public int getReadyForQueryToIgnore() {
        return readyForQueryToIgnore;
    }

    public void resetReadyForQueryToIgnore() {
        readyForQueryToIgnore = 0;
    }

    public void incrementeReadyForQueryToIgnore() {
        readyForQueryToIgnore ++;
    }

    public void decrementeReadyForQueryToIgnore() {
        readyForQueryToIgnore --;
    }

    public void resetCurrentOperation() {
        setCurrentOperation(null);
        setPromise(null);
        resetRowDescription();
    }

    public void resetCurrentCommand() {
        resetCurrentOperation();
        resetBufferedStatements();
        resetCommandResultsToIgnore();
        resetReadyForQueryToIgnore();
    }

    public void reset() {
        setDatabaseName(null);
        setInTransaction(false);
        setInDatasetCreation(false);
        setTransferMode(null);
        resetCurrentCommand();
    }

}
