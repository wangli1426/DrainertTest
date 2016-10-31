package edu.illinois.adsc;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Created by robert on 31/10/16.
 */
public class DranerTester {

    private LinkedBlockingQueue<Tuple> inputQueue = new LinkedBlockingQueue<Tuple>();
    private ArrayList<LinkedBlockingQueue<Tuple>> outputQueues;
    private int numberOfWorkerThreads;
    private int numberOfTuplesInInputQueue;


    public DranerTester(int numberOfTuplesInInputQueue, int numberOfWorkerThreads) {
        this.numberOfWorkerThreads = numberOfWorkerThreads;
        this.numberOfTuplesInInputQueue = numberOfTuplesInInputQueue;
        outputQueues = new ArrayList<LinkedBlockingQueue<Tuple>>();
        for(int i = 0; i < 5; i++) {
            outputQueues.add(new LinkedBlockingQueue<Tuple>());
        }
    }

    public void startTest() {
        testWithDrainer();
        testWithoutDrainer();
    }

    void testWithDrainer() {
        populateInputQueue();
        Thread[] threads = new Thread[numberOfWorkerThreads];

        for(int i = 0; i < threads.length; i++ ) {
            threads[i] = new Thread(new WorkerThreadWithDrainer());
        }

        final long inputSize = inputQueue.size();
        final long startTime = System.currentTimeMillis();

        for(int i = 0; i < threads.length; i++) {
            threads[i].start();
        }

        try {
            for (int i = 0; i < threads.length; i++) {
                threads[i].join();
            }
        } catch (InterruptedException e ) {
            e.printStackTrace();
        }

        assert inputQueue.size() == 0: "Input queue is not empty.!";
        System.out.println(String.format("Throughput with drainer: %4.4f", inputSize / (double) (System.currentTimeMillis() - startTime) * 1000));
    }

    void testWithoutDrainer() {
        populateInputQueue();
        Thread[] threads = new Thread[numberOfWorkerThreads];

        for(int i = 0; i < threads.length; i++ ) {
            threads[i] = new Thread(new WorkerThreadWithoutDrainer());
        }

        final long inputSize = inputQueue.size();
        final long startTime = System.currentTimeMillis();

        for(int i = 0; i < threads.length; i++) {
            threads[i].start();
        }

        try {
            for (int i = 0; i < threads.length; i++) {
                threads[i].join();
            }
        } catch (InterruptedException e ) {
            e.printStackTrace();
        }

        assert inputQueue.size() == 0: "Input queue is not empty.!";
        System.out.println(String.format("Throughput without drainer: %4.4f", inputSize / (double) (System.currentTimeMillis() - startTime) * 1000));
    }



    void populateInputQueue() {
        inputQueue.clear();
        for(int i = 0; i < numberOfTuplesInInputQueue; ++i) {
            inputQueue.add(new Tuple());
        }
    }

    class WorkerThreadWithDrainer implements Runnable {

        public void run() {
            ArrayList<Tuple> drainer = new ArrayList<Tuple>();
            ArrayList<ArrayList<Tuple>> tempOutputQueues = new ArrayList<ArrayList<Tuple>>();
            for(int i = 0; i < outputQueues.size(); i++) {
                tempOutputQueues.add(new ArrayList<Tuple>());
            }

            while(!inputQueue.isEmpty()) {
                inputQueue.drainTo(drainer, 1024);
                for (Tuple tuple : drainer) {
                    tempOutputQueues.get(Math.abs(tuple.tupleId.hashCode()) % tempOutputQueues.size()).add(tuple);
                }
                for(int i = 0; i < tempOutputQueues.size(); i++) {
                    outputQueues.get(i).addAll(tempOutputQueues.get(i));
                    tempOutputQueues.get(i).clear();
                }
                drainer.clear();
            }
        }
    }

    class WorkerThreadWithoutDrainer implements Runnable {

        public void run() {
            while(!inputQueue.isEmpty()) {
                try {
                    Tuple tuple = inputQueue.poll(1, TimeUnit.MILLISECONDS);
                    if(tuple != null) {
                        outputQueues.get(Math.abs(tuple.tupleId.hashCode()) % outputQueues.size()).put(tuple);
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }


}
