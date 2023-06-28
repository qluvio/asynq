# Tasks

## Task state 

color legend

* gray: actual asynq states reflected in the DB of the broker
* lightblue: internal states not reflected in the DB of the broker

![Task state](task_state.png)

## Server components

The asynq server work is made by several components:

| component     | description                                                                                                        |
|---------------|--------------------------------------------------------------------------------------------------------------------|
| broker        | access to storage (sqlite, redis)                                                                                  |
| processor     | poll queues and process tasks                                                                                      |
| forwarder     | moves scheduled and retry tasks to pending state                                                                   |
| syncer        | queues up failed requests to the broker and retry them to sync state between the background process and the broker |
| heartbeater   | writes process info periodically to the broker                                                                     |
| subscriber    | subscribes to the broker for notifications of canceled tasks                                                       |
| recoverer     | send active tasks with exceeded dead lines to retry or archive                                                     |
| healthchecker | notifies user components of health state                                                                           |
| janitor       | deletes expired completed tasks                                                                                    |


**Components intervals and actions.**<br>
Names of interval are those found in a server configuration.

* server
   * configurable interval: `ShutdownTimeout` 
* processor     
   * configurable interval: `ProcessorEmptyQSleep`
   * actions:
      * broker.Dequeue
      * broker.Requeue
      * broker.MarkAsComplete
      * broker.Done
      * broker.Retry
      * broker.Archive
* forwarder      
   * configurable interval: `ForwarderInterval`
   * action: broker.ForwardIfReady              
* syncer        
   * configurable interval: `SyncerInterval`                                                       
   * action: receives failed requests from the processor and retries them 
* heartbeater   
   * configurable interval: `HeartBeaterInterval`               
   * action: broker.WriteServerState            
* subscriber    
   * configurable intervals: 
      * `SubscriberRetryTimeout`
      * `PubsubPollingInterval` - for (r/s)qlite **on client config**                                     
   * action: broker.CancelationPubSub           
* recoverer      
   * configurable intervals: 
      * `RecovererInterval`
      * `RecovererExpiration`
   * note: at each `RecovererInterval`, evaluates tasks that have expired `RecovererExpiration` seconds ago or earlier
   * action: broker.ListDeadlineExceeded then calls broker.Retry | broker.Archive
* healthchecker 
   * configurable interval: `HealthCheckInterval`
   * action: broker.ping                        
* janitor       
   * configurable interval: `JanitorInterval`
   * action: broker.DeleteExpiredCompletedTasks 


