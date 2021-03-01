package cp1.solution;

import cp1.base.ResourceId;
import cp1.base.ResourceOperation;

import java.util.ArrayList;

// klasa pomocnicza trzymajaca elementarne rzeczy
// ktore wymagamy od transakcji
public class Transaction {

    // ID zasobu na ktory czeka (moze byc nullem wtedy na nic nie czeka)
    private ResourceId isWaitingFor;

    // watek ktory obsluguje ta transakcje
    private final Thread isOperatedBy;

    // informacja o tym czy transakcja zostala anulowana
    private boolean isAborted;

    // lista zasobow na ktorych wykonywalem kolejne operacje
    // (zasoby moga sie powtarzac)
    private final ArrayList<ResourceId> resourcesHistory;

    // lista kolejnych operacji ktore wykonywalem na zasobach
    private final ArrayList<ResourceOperation> operationsHistory;

    // lista zasobow ktore mam pod swoja kontrola, nie powtarzaja sie
    private final ArrayList<ResourceId> controlledResources;

    // czas w ktorym zasob powstal
    private final long timeCreated;

    Transaction(long timeCreated) {
        this.timeCreated = timeCreated;
        isOperatedBy = Thread.currentThread();
        controlledResources = new ArrayList<>();
        resourcesHistory = new ArrayList<>();
        operationsHistory = new ArrayList<>();

        // poczatkowo nie jest anulowany
        isAborted = false;

        // poczatkowo na nic nie czeka
        isWaitingFor = null;
    }

    public long getTimeCreated() {
        return timeCreated;
    }

    public boolean isAborted() {
        return isAborted;
    }

    public long getID() {
        return isOperatedBy.getId();
    }

    public ResourceId whatBlocksTrans() {
        return isWaitingFor;
    }

    public void isNowWaitingFor(ResourceId rid) {
        isWaitingFor = rid;
    }

    ArrayList<ResourceId> getResourcesOperatedOn() {
        return resourcesHistory;
    }

    ArrayList<ResourceOperation> getDoneOperations() {
        return operationsHistory;
    }

    ArrayList<ResourceId> getControlledResources() {
        return controlledResources;
    }

    public Thread getThread() {
        return isOperatedBy;
    }

    public void abort() {
        isAborted = true;
    }

    // dodanie kolejnej operacji, zasob na ktorym zostala wykonana jest wrzucany
    // na resourcesHistory, natomiast operacja na operationsHistory
    public void addOperation(ResourceId rid, ResourceOperation operation) {
        resourcesHistory.add(rid);
        operationsHistory.add(operation);
    }

    // dodaje nowy zasob do kolekcji, funkcja wywolana tylko gdy jestem pewien ze
    // nie posiadam tego zasobu
    public void addNewResource(ResourceId rid) {
        controlledResources.add(rid);
    }

}
