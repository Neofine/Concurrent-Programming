package cp1.solution;
import cp1.base.*;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;

public class TransManager implements cp1.base.TransactionManager {

    // sluzy do okreslenia czasu rozpoczecia transakcji
    private final LocalTimeProvider timeProvider;

    // mapa ID watkow oraz ich posiadanych transakcji
    // watek moze miec tylko 1 transakcje aktywna
    private volatile ConcurrentHashMap<Long, Transaction> controlledTrans;

    // mapa przedstawiajaca dla ID zasobow przez jaka transakcje sa kontrolowane
    private final ConcurrentHashMap<ResourceId, Transaction> controlledBy;

    // mapa przedstawiajaca dla ID zasobow jaki zasob sie za tym Id kryje
    private final ConcurrentHashMap<ResourceId, Resource> idToResource;

    // mapa przedstawiajaca dla ID zasobow ile transakcji czeka na dostep do niego
    private final ConcurrentHashMap<ResourceId, Integer> waitsOn;

    // mapa przedstawiajaca dla ID zasobow semafor, na ktorym czekaja
    // transakcje ktore chca ten zasob posiasc
    private final ConcurrentHashMap<ResourceId, Semaphore> queueForResource;

    // mapa przedstawiajaca dla ID watku jaka transakcje posiada, jest potrzebna
    // gdyz jeden watek nie moze jednoczasowo 2 transakcji w 2 roznych TM obslugiwac
    private static final ConcurrentHashMap<Long, Transaction> activeTransactions = new ConcurrentHashMap<>();

    // lista transakcji ktore bede budzil podczas zwalniania swoich zasobow
    // budzenie nastepuje kaskadowo wiec potrzebuje je gdzies trzymac
    private final ArrayList<ResourceId> waking;

    // iterator pomocniczy do listy waking
    private int it;

    // glowny semafor klasy, zapewnia sekcje krytyczna (np podczas szukania deadlocka)
    private static final Semaphore mutex = new Semaphore(1, true);

    TransManager(Collection<Resource> resources, LocalTimeProvider timeProvider) {
        idToResource = new ConcurrentHashMap<>();
        controlledTrans = new ConcurrentHashMap<>();
        controlledBy = new ConcurrentHashMap<>();
        queueForResource = new ConcurrentHashMap<>();
        controlledTrans = new ConcurrentHashMap<>();
        waitsOn = new ConcurrentHashMap<>();
        waking = new ArrayList<>();

        for (Resource rsc : resources) {
            idToResource.put(rsc.getId(), rsc);
            // ustawiam semafory na 0 poczatkowo, nikt nie ma dostepu
            queueForResource.computeIfAbsent(rsc.getId(), k -> new Semaphore(0, true));
            // ilosc czekajacych na dowolny zasob jest rowna 0
            waitsOn.put(rsc.getId(), 0);
        }

        this.timeProvider = timeProvider;
    }

    // rozpoczecie transakcji
    @Override
    public void startTransaction() throws AnotherTransactionActiveException {
        long threadID = Thread.currentThread().getId();
        // jezeli watek ma aktywna transakcje na jakimkolwiek MT to throwuje
        if (activeTransactions.containsKey(threadID)) {
            throw new AnotherTransactionActiveException();
        }
        Transaction thisTrans = new Transaction(timeProvider.getTime());
        controlledTrans.put(threadID, thisTrans);
        activeTransactions.put(threadID, thisTrans);
    }

    // funkcja rekurencyjnie sprawdzajaca deadlock, zwraca nulla jezeli nie
    // znaleziono transakcji go powodujacej a w przeciwnym wypadku zwraca
    // najpozniej stworzona transakcje go powodujaca
    Transaction checkDeadlock(Transaction original, Transaction now, Transaction latest) {
        // aktualizuje kandydata na najpozniej odpalona transakcje
        if (now.getTimeCreated() > latest.getTimeCreated() ||
                (now.getTimeCreated() == latest.getTimeCreated() &&
                        now.getID() > latest.getID())) {
            latest = now;
        }
        // jezeli nie blokuje transakcji obecnej
        if (now.whatBlocksTrans() == null) {
            return null;
        }
        // jezeli transakcja oryginalna blokuje obecna transakcje
        else if (controlledBy.get(now.whatBlocksTrans()) == original) {
            return latest;
        }
        // jezeli ktos blokuje lecz to nie jest oryginalna transakcja
        else {
            return checkDeadlock(original, controlledBy.get(now.whatBlocksTrans()), latest);
        }
    }

    // funkcja wykonujaca operacje gdy juz transakcja ma ten zasob
    private void applyOperation(ResourceId rid, ResourceOperation operation, Transaction trans)
            throws ResourceOperationException, InterruptedException{
        Resource operationOn = idToResource.get(rid);
        // jak execute zakonczy sie powodzeniem to dodaje to wykonanych operacji ta operacje
        trans.addOperation(rid, operation);

        // jak w linijce nizej nastapi blad to throwuje sie ResourceOperationException
        operation.execute(operationOn);

        // podczas execute'a watek moze zostac przerwany to po nim wpadnie w tego if'a
        if (Thread.currentThread().isInterrupted()) {
            operation.undo(operationOn);
            throw new InterruptedException();
        }
    }

    // funkcja probujaca wykonac operacje na zasobie
    @Override
    public void operateOnResourceInCurrentTransaction(ResourceId rid, ResourceOperation operation)
            throws NoActiveTransactionException, UnknownResourceIdException, ActiveTransactionAborted,
            ResourceOperationException, InterruptedException {
        // jezeli miedzy funkcjami watek sie zinterruptowal to ignoruje
        // ta flage
        Thread.interrupted();

        long threadID = Thread.currentThread().getId();

        // opuszczam mutexa
        mutex.acquire();

        // sprawdz czy ma aktywna transakcje
        if (!controlledTrans.containsKey(threadID)) {
            mutex.release();
            throw new NoActiveTransactionException();
        }

        // jezeli zasob nie jest pod kontrola MT
        if (!idToResource.containsKey(rid)) {
            mutex.release();
            throw new UnknownResourceIdException(rid);
        }

        // obecna transakcja
        Transaction thisTrans = controlledTrans.get(threadID);

        // sprawdz czy zostala anulowana wczesniej
        if (thisTrans.isAborted()) {
            mutex.release();
            throw new ActiveTransactionAborted();
        }

        // jezeli transakcja juz posiada ten zasob lub nikt go nie uzywa
        if (!controlledBy.containsKey(rid) || controlledBy.get(rid).getID() == threadID) {
            // jezeli nikt nie uzywa do transakcja dodaje ten zasob
            // do listy zasobow pod swoja kontrola
            if (!controlledBy.containsKey(rid))
                thisTrans.addNewResource(rid);

            controlledBy.put(rid, thisTrans);
            mutex.release();
        }
        // jezeli ktos inny uzywa tego zasobu
        else {
            // sprawdz czy zakleszczenie nastepuje
            Transaction latestCausingDeadlock = checkDeadlock(thisTrans, controlledBy.get(rid), thisTrans);
            // jezeli faktycznie nastepuje
            if (latestCausingDeadlock != null) {
                // transakcja zostaje anulowana
                latestCausingDeadlock.abort();
                // kiedy to ja powoduje deadlock
                if (thisTrans == latestCausingDeadlock) {
                    mutex.release();
                    throw new ActiveTransactionAborted();
                }
            }

            // zaznaczam ze czekam na dany zasob (potrzebne do pozniejszego
            // sprawdzania deadlocka)
            thisTrans.isNowWaitingFor(rid);

            // zaznaczam ze czekam na dany zasob w kolejce
            waitsOn.put(rid, waitsOn.get(rid) + 1);

            // przerywam watek (wiem ze to nie ja spowodowalem deadlocka) ktory powoduje
            // deadlocka, dzieje sie to tutaj, gdyz mapa waitsOn musi
            // koniecznie zostac zaktualizowana, jezeli watek anulowany
            // oddalby teraz szybko swoje zasoby, rozwniez dekrementuje ilosc
            // czekajacych watkow na rid (gdyz ten ktorego anuluje na pewno czeka na jakis)
            if (latestCausingDeadlock != null) {
                waitsOn.put(latestCausingDeadlock.whatBlocksTrans(),
                        waitsOn.get(latestCausingDeadlock.whatBlocksTrans()) - 1);
                latestCausingDeadlock.isNowWaitingFor(null);
                latestCausingDeadlock.getThread().interrupt();
            }

            mutex.release();

            try {
                // czekam tutaj na zasob ktory umozliwi mi wykonanie tej operacji
                queueForResource.get(rid).acquire();
                // tu juz InterruptedException nie nastapi oraz transakcja nie jest anulowana
                waitsOn.put(rid, waitsOn.get(rid) - 1);

                // zaznaczam ze juz na nic nie czekam
                thisTrans.isNowWaitingFor(null);

                // dodaje zasob pod swoja kontrole
                thisTrans.addNewResource(rid);
                controlledBy.put(rid, thisTrans);

                // tutaj dziedzicze sekcje krytyczna
                if (it + 1 != waking.size()) {
                    it++;
                    queueForResource.get(waking.get(it)).release();
                }
                // jak juz nie mam kogo obudzic to zwalniam mutexa
                else {
                    mutex.release();
                }
            }
            // jezeli podczas czekania na semaforze zostalismy zinterruptowani
            catch (InterruptedException e) {
                // zaznaczamy ze juz na nic nie czekamy
                thisTrans.isNowWaitingFor(null);
                throw new ActiveTransactionAborted();
            }
        }
        // tutaj posiadam juz zasob do swojej dyspozycji
        applyOperation(rid, operation, thisTrans);
    }

    // funkcja uzywana przez commit oraz rollback, zwraca zasoby
    // gdyz juz ta transakcja nie bedzie ich potrzebowala
    void freeResources(Transaction thisTrans) {
        // jezeli miedzy funkcjami watek sie zinterruptowal to ignoruje
        // ta flage
        Thread.interrupted();

        // potrzebuje to zrobic pod mutexem
        try {
            mutex.acquire();
        }
        // zgodnie z trescia zadania nigdy nie zostanie watek w tym
        // momencie przerwany
        catch (InterruptedException e) {
            System.out.println("This is never going to happen");
        }

        // czyszcze tablice zasobow na ktore czekaja transakcje
        waking.clear();

        // lista roznych zasobow ktore mam pod swoja kontrola
        ArrayList<ResourceId> toBeFreed = thisTrans.getControlledResources();

        for (ResourceId rid : toBeFreed) {
            controlledBy.remove(rid);
            // jezeli ktos czeka na ten zasob
            if (waitsOn.get(rid) != 0) {
                waking.add(rid);
            }
        }

        // dziedzicze sekcje krytyczna transakcjom ktore czekaja
        // na moj zasob
        if (waking.size() != 0) {
            it = 0;
            // rozpoczynam ten proces
            queueForResource.get(waking.get(it)).release();
        }
        // jezeli nikogo nie bede budzil (powyzej nie musze zwalniac
        // mutexa gdyz transakcja ktora juz nie ma nic do obudzenia
        // zwolni go)
        else {
            mutex.release();
        }

        // juz nie kontroluje tej transakcji
        controlledTrans.remove(thisTrans.getID());
        // juz ta transakcja nie jest aktywna
        activeTransactions.remove(thisTrans.getID());
    }

    // funkcja zatwierdzajaca operacje wykonane na obecnej transakcji,
    // jak nie rzuci wyjatku to zwalnia rowniez posiadane zasoby
    @Override
    public void commitCurrentTransaction() throws NoActiveTransactionException, ActiveTransactionAborted {
        long threadID = Thread.currentThread().getId();

        // jezeli nie kontroluje tego watku
        if (!controlledTrans.containsKey(threadID)) {
            throw new NoActiveTransactionException();
        }

        Transaction thisTrans = controlledTrans.get(threadID);

        // jezeli transakcja zostala anulowana
        if (thisTrans.isAborted()) {
            throw new ActiveTransactionAborted();
        }

        freeResources(thisTrans);
    }

    // funkcja cofajaca operacje wykonane na obecnej transakcji,
    // jak transakcja znajduje sie pod kontrola MT to
    // zwalnia rowniez zasoby jakie transakcja posiadala
    @Override
    public void rollbackCurrentTransaction() {
        long threadID = Thread.currentThread().getId();

        Transaction thisTrans = controlledTrans.get(threadID);

        // jezeli nie kontroluje transakcji z tego watku
        if (thisTrans == null)
            return;

        // lista kolejnych zasobow na ktorych wykonywalem operacje
        ArrayList<ResourceId> resourceList = thisTrans.getResourcesOperatedOn();
        // lista kolejnych operacji ktore wykonywalem na zasobach
        ArrayList<ResourceOperation> operationsList = thisTrans.getDoneOperations();

        // ide od konca (od ostatnich zmian jakie zrobilem do tych najwczesniejszych)
        for (int i = resourceList.size() - 1; i >= 0; i--) {
            Resource operationOn = idToResource.get(resourceList.get(i));
            operationsList.get(i).undo(operationOn);
        }

        freeResources(thisTrans);
    }

    // funkcja zwracajaca booleana czy dana transakcja jest aktywna
    @Override
    public boolean isTransactionActive() {
        return controlledTrans.containsKey(Thread.currentThread().getId());
    }

    // funkcja zwracajaca booleana czy dana transakcja jest anulowana
    @Override
    public boolean isTransactionAborted() {
        return isTransactionActive() &&
                controlledTrans.get(Thread.currentThread().getId()).isAborted();
    }
}
