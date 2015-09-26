/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * ChapelScheduler.cpp
 *
 * A mesos-scheduler for Chapel programs!
 *
 * ct.clmsn at gmail dot com
 * 24AUG2015
 *
 */

#include "ChapelScheduler.hpp"

#include <iostream>
#include <thread>
#include <cmath>

#include <stdio.h>
#include <unistd.h>
#include <pwd.h>

#include <sys/types.h>
#include <sys/wait.h>

#include <netdb.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include <stout/stringify.hpp>

using namespace std;

const string os_pathsep("/");

static void shutdown();
static void SIGINTHandler();

string getChapelLeaderEnvVars(const string& leader_hostname);
string getCleanRemoteCmdString(const string& remote_cmd_str);
CommandInfo::URI getCommandInfoURI(const string& remote_exec_path);

// external variable, reference to environment variables set in the shell running the scheduler
extern char** environ;

// scheduler driver ref
MesosSchedulerDriver* schedulerDriver = NULL;

vector<string> stringToVector(const string& str) {
   const char* data = str.c_str();
   string lenStr = data;
   size_t len = numify<size_t>(data).get();
   data += lenStr.length()+1;

   vector<string> result;

   for(size_t i = 0; i < len; i++) {
      string s = data;
      data+=s.length()+1;
      result.push_back(s);
   }

   return result;
}

ChapelScheduler::ChapelScheduler(const int cpus_req, 
                                 const uint64_t mem_req, 
                                 const double cores_req, 
                                 const string& leader_hostname, 
                                 const string& executable, 
                                 const string& remote_cmd, 
                                 const string& usr, 
                                 const int num_attempts) 
  : cpusReq(cpus_req), 
    memReq(mem_req), 
    coresReq(cores_req), 
    mesosReq("cpus:" + stringify<double>(cores_req) + ";mem:" + stringify<int>(mem_req)),
    leaderHostname(leader_hostname),
    remoteCmd(getChapelLeaderEnvVars(leader_hostname) + " " + getCleanRemoteCmdString(remote_cmd)),
    exec(executable),
    user_id(usr),
    tasksLaunched(0),
    tasksFinished(0),
    tasksFailed(0),
    tasksExecError(false),
    numAttempts(num_attempts),
    frameworkMessagesReceived(0) 
{
   // clean up command passed in from Chapel program/GASNet - has a lot of extra quotation marks
   const CommandInfo::URI chpl_real_uri = getCommandInfoURI(exec);
   chplCmdInfo.set_value(remoteCmd); // set command that the remote worker (Executor) will run through system()
   chplCmdInfo.add_uris()->CopyFrom(chpl_real_uri); // set the uri for the remote worker (Executor) to pull the a.out_real from... 
}

void ChapelScheduler::registered(SchedulerDriver* driver, 
                                 const FrameworkID& frameworkId, 
                                 const MasterInfo& masterInfo) {
   cout << "Chapel program registered" << endl;
}

void ChapelScheduler::reregistered(SchedulerDriver* driver, 
                                   const MasterInfo& masterInfo) {
   cout << "Chapel program re-registered!" << endl;
}

void ChapelScheduler::disconnected(SchedulerDriver* driver) {
   cout << "Chapel program disconnected!" << endl;
}

inline string getMesosResourceField(const string& mesosResourceString, 
                                    const string& field) 
{
   const string fieldStr = (field.find(":") == string::npos) ? field + ":" : field;
   const size_t fieldsz = fieldStr.size();
   const size_t i = mesosResourceString.find(fieldStr);
   const size_t j = mesosResourceString.find(";");
   const string val = (string::npos != j) ? mesosResourceString.substr(i+fieldsz, j-(i+fieldsz)) : mesosResourceString.substr(i+fieldsz);
   return val;
}

#define LIBJAVADIR
  #define LDLIBPATH ""
#else
  #define LDLIBPATH ":" + LIBJAVADIR
#endif

inline int round(double x) { std::floor(x + 0.5); }

inline string getChapelLeaderEnvVars(const string& leader_hostname) {
   const string gasnet_stdout_routing("CSPAWN_ROUTE_OUTPUT=1");
   const string gasnet_master_ip("GASNET_MASTERIP="+leader_hostname);
   const string amudp_master_ip("AMUDP_MASTERIP="+leader_hostname);
   const string envVars = "CHPL_COMM=gasnet " + gasnet_master_ip + " GASNET_SPAWNFN=C " + amudp_master_ip + " " + gasnet_stdout_routing + " LD_LIBRARY_PATH=$LD_LIBRARY_PATH" + LDLIBPATH;

   unsigned int n = std::thread::hardware_concurrency();
   envVars += (n > 0) ? " CHPL_RT_NUM_THREADS_PER_LOCALE=" + stringify<double>(round(coresReq)*n) : "";

   return envVars;
}

const static string SH_CMD_STR = "/bin/sh -c \"";

inline string getCleanRemoteCmdString(const string& remote_cmd_str) {
   const size_t end_of_sh_pos = remote_cmd_str.find(SH_CMD_STR) + SH_CMD_STR.size();
   string remote_cmd_args = remote_cmd_str.substr(end_of_sh_pos);
   const size_t pipe_err_pos = remote_cmd_args.find("\" |");
   remote_cmd_args.replace(pipe_err_pos, 3, " |");
   return remote_cmd_args;
}

inline CommandInfo::URI getCommandInfoURI(const string& remote_exec_path) {
   CommandInfo::URI chpl_real_uri;
   chpl_real_uri.set_value(remote_exec_path);
   chpl_real_uri.set_executable(true);
   return chpl_real_uri;
}

inline size_t getPendingTsksSize(map<string, vector<TaskInfo>>& pendingTsks) {
   size_t toret = 0;

   for(map<string, vector<TaskInfo>>::iterator i = pendingTsks.begin(); i != pendingTsks.end(); i++) {
      const vector<TaskInfo>& tsks = i->second;
      toret+=(size_t)tsks.size();
   }

   return toret;
}

void ChapelScheduler::resourceOffers(SchedulerDriver* driver, 
                                     const vector<Offer>& offers) 
{
   // offers only contain resources describing a single node -> for more details read include/mesos/mesos.proto
   // 
   cout << "***\tProcessing Offers!" << endl;

   const int remainingCpusReq = cpusReq - launchedTsks.size();

   if(remainingCpusReq == 0) {

      for(size_t k = 0; k < offers.size(); k++) {
         const Offer& offer = offers[k];
         driver->declineOffer(offer.id());
      }

      cout << "\t\tChapelScheduler declined offer because resource requirements satisfied" << endl;
   }

   // cycle through all the offers and resource a task
   // each offer corresponds to a single compute node
   //
   const static Resources TASK_RESOURCES = Resources::parse(mesosReq).get();
   vector<TaskInfo> tsks;

   for(size_t i = 0; i < offers.size(); i++) {
      const Offer& offer = offers[i];

      if(tsks.size() == remainingCpusReq) {
         driver->declineOffer(offer.id());
         continue; // need to cycle through the remaining offers and decline them
      }

      Resources remaining = offer.resources();

      /* attempting to exercise multi-tenancy capabilities in mesos
       * given an offer from a node, try to maximize the number of jobs
       * that can be allocated to that node given the job's resource
       * requirements
       *
       * if the desired number of nodes and jobs are met, then launch
       * all the jobs on that node's offer
       *
       * this means some nodes will get multiple tasks assigned for
       * execution 
       */

      vector<TaskInfo> tol;

      while(remaining.flatten().contains(TASK_RESOUCES) && ((remainingCpusReq-tsks.size()) > 0)) {
         const string tid = stringify<size_t>(tsks.size());

         TaskInfo task;
         task.set_name("Chapel Remote Program Task\t" + tid);

         task.mutable_task_id()->set_value(tid);
         task.mutable_slave_id()->MergeFrom(offer.slave_id());
         task.mutable_command()->MergeFrom(chplCmdInfo);
         task.mutable_resources()->MergeFrom(TASK_RESOURCES);

         task.set_data(remoteCmd);
         tol.push_back(task); // tol means "to launch"
         tsks.push_back(task); // tsks tracks tasks launched for framework termination purposes

         remaining-=TASK_RESOURCES;
         tasksLaunched+=1;

         cout << "\t\t+++\tLaunching # of Tasks!\t" << tol.size() << " of " << tasksLaunched << endl;
      }

      // after all the tasks for this offer have been "resourced"
      // launch the tasks using this offer.id
      //
      driver->launchTasks(offer.id(), tol);
   }

   const size_t pendingTsksSize = tsks.size();
   cout << endl << "\tAcquired # tasks " << pendingTsksSize << " required # of tasks " << cpusReq << " remaining required # tasks " << remainingCpusReq << endl << endl;
   
   if(pendingTsksSize > 0) {
      for(vector<TaskInfo>::iterator i = tsks.begin(); i != tsks.end(); i++) {
         launchedTsks.insert(make_pair(i->task_id().value(), *i));
      }
   }

}

void ChapelScheduler::offerRescinded(SchedulerDriver* driver, const OfferID& offerId) {
   cout << "ChapelScheduler::offerRescinded" << endl;
   terminateAllTasks(driver);
   driver->stop();
}

void ChapleScheduler::terminateAllTasks(SchedulerDriver* driver) {
   for(map<string, TaskInfo>::iterator i = launchedTsks.begin(); i != launchedTsks.end(); i++) {
      cout << "\tChapel Task " << i->first << " notified to terminate" << endl;
      TaskID tid;
      tid.set_value(i->first);
      driver->killTask(tid);
   }
}

void ChapelScheduler::statusUpdate(SchedulerDriver* driver, const TaskStatus& status) {

   if (status.state() == TASK_FINISHED) {
      tasksFinished+=1;
      cout << "ChapelScheduler::statusUpdate\tTask " << status.task_id().value() << " finished of # tasksLaunched " << tasksLaunched << " # finished " << tasksFinished << endl;
   }

   if (status.state() == TASK_FAILED) {
      cout << "ChapelScheduler::statusUpdate\tTask " << status.task_id().value() << " FAILED!" << endl;
      terminateAllTasks(schedulerDriver);
      taskExecError=true;
      driver->stop();
   }

   if (status.state() == TASK_LOST) {
      cout << "ChapelScheduler::statusUpdate\tTask " << status.task_id().value() << " LOST!" << endl;
      terminateAllTasks(schedulerDriver);
      taskExecError=true;
      map<string, TaskInfo>::iterator rm = launchedTsks.find(status.task_id().value());
      if(rm != launchedTsks.end()) { launchedTsks.erase(rm); }
   }

   if (status.state() == TASK_KILLED) {
      cout << "ChapelScheduler::statusUpdate\tTask " << status.task_id().value() << " KILLED!" << endl;
      terminateAllTasks(schedulerDriver);
      taskExecError=true;
      map<string, TaskInfo>::iterator rm = launchedTsks.find(status.task_id().value());
      if(rm != launchedTsks.end()) { launchedTsks.erase(rm); }
   }

   cout << "ChapelScheduler::statusUpdate\tMet termination criteria?\t" << (tasksFinished == tasksLaunched) << " " << tasksFinished << " " << tasksLaunched << " " << taskExecError << endl;

   if( taskExecError || ((tasksFinished == tasksLaunched) || (tasksFinished == cpusReq))) {

      if(tasksLaunched < tasksFinished) {
         cout << "ChapelScheduler::statusUpdate\tError getting nodes launched for the batch job! Try re-running the code!" << endl;
      }

      // Wait to receive any pending framework messages
      //
      // If some framework messages are lost, it may hang indefinitely
      // to solve the indefinite "hang", numAttempts caps out and then 
      // terminates the while loop.
      //
      int attempts = 0;
      while(tasksFinished != tasksLaunched && attempts < numAttempts) {
         cout << "ChapelScheduler::statusUpdate\tExecution halted! Waiting for remote nodes to catch up! Attempts\t" << attempts << endl;
         sleep(1);
         attempts+=1;
      }

      cout << "All Chapel task for this framework instance are complete! Shutting down!" << endl;
      driver->stop();
   }
}



void ChapelScheduler::frameworkMessage(SchedulerDriver* driver, 
                                       const ExecutorID& executorId, 
                                       const SlaveID& slaveId, 
                                       const string& data) 
{
   vector<string> strVector = stringToVector(data);
   string taskId = strVector[0];
   string url = strVector[1];

   if(executorId.value().size() > 0) {
      cout << "Framework message received: " << taskId << endl;
   }

   frameworkMessagesReceived+=1;
}

void ChapelScheduler::slaveLost(SchedulerDriver* driver, 
                                const SlaveID& slaveId) 
{
   cout << "Framework lost a follower!" << endl;
   taskExecError=true;
   shutdown();
   driver->stop();
}

void ChapelScheduler::executorLost(SchedulerDriver* driver, const ExecutorID& executorId, const SlaveID& slaveId, int status) {
   cout << "Lost a follower!" << endl;
   taskExecError=true;
   shutdown();
   driver->stop();
}

void ChapelScheduler::error(SchedulerDriver* driver, const string& message) {
   cout << "error in scheduler or scheduler driver!" << endl;
   cout << message << endl;
   taskExecError=true;
   shutdown();
   driver->stop();
}

static void shutdown() {
   cout << "Chapel is shutting down" << endl;
}

static void SIGINTHandler(int signum) {
   if(schedulerDriver != NULL) {
      cout << "SIGINTHandler called!" << endl;
      shutdown();
      schedulerDriver->stop();
      delete schedulerDriver;
   }

   exit(0);
}

static string getHostname() {
   char hostname_cstr[1024];
   hostname_cstr[1023] = '\0';
   gethostname(hostname_cstr, 1023);
   struct hostent* h;
   h = gethostbyname(hostname_cstr);
   const string hostname(h->h_name);
   return hostname;
}

static string getIP() {
   string hstname = getHostname();
   struct hostent* h;
   h = gethostbyname(hstname.c_str());
   const string hostname(inet_ntoa(*((struct in_addr*)h->h_name)));
   return hostname;
}

#define shift argc--,argv++
int main(int argc, char** argv) {
   string execpath, master, remotecmd;
   uint64_t mem = 64;
   double cpus = -1.0, cores = -1.0;

   shift;
   while(true) {
      const string s = argc > 0 ? argv[0] : "--help";

      if(argc > 1 && s == "--exec-uri") {
         execpath = argv[1];
         shift; shift;
      }
      else if(argc > 1 && s == "--master") {
         master = argv[1];
         shift; shift;
      }
      else if(argc > 1 && s == "--cpus") {
         cpus = (double)stof(argv[1]);
         assert(cpus > 0.0);
         shift; shift;
      }
      else if(argc > 1 && s == "--mem") {
         mem = Bytes::parse(argv[1]).get().megabytes();
         shift; shift;
      }
      else if(argc > 1 && s == "--cores") {
         cores = (double)stof(argv[1]);
         assert(cores> -1.0);
         shift; shift;
      }
      else if(argc > 1 && s == "--remote-cmd") {
         remotecmd = argv[1];
         shift; shift;
      }
      else { break; }
   }

   if(master.length() == 0 || execpath.length() == 0 || cpus == -1) {
      printf("Usage: chapel-scheduler --master <ip:port|required> --exec-uri <uri-to-the-location-of-a.out_real|required> --cpus <number-of-nodes|%N> --cores <number-of-cores to use|optional default is '2'> --mem <amt-of-ram-per-node|optional default is '64MB'> --remote-cmd %C\n");
      exit(1);
   }
   const string& hostname = getHostname();

   const uid_t uid = geteuid();
   const struct passwd* pw = getpwuid(uid); // don't free this pointer! check the docs of this function to confirm!

   FrameworkInfo framework;
   framework.set_user(pw->pw_name);
   framework.set_name("Chapel Framework");
   //framework.set_role(role);
   framework.set_principal("cpp");

   ChapelScheduler scheduler(cpus, mem, cores, hostname, execpath, remotecmd, pw->pw_name);

   // set up signal handler for clean shutdown
   //
   struct sigaction action;
   action.sa_handler = SIGINTHandler;
   sigemptyset(&action.sa_mask);
   action.sa_flags = 0;
   sigaction(SIGINT, &action, NULL);
   sigaction(SIGHUP, &action, NULL);
   sigaction(SIGQUIT, &action, NULL);
   sigaction(SIGCHLD, &action, NULL);
   sigaction(SIGSTOP, &action, NULL);
   sigaction(SIGABRT, &action, NULL);

   schedulerDriver = new MesosSchedulerDriver(&scheduler, framework, master);
   const uint8_t status = schedulerDriver->run() == DRIVER_STOPPED ? 0 : 1;

   if(status) {
      cout << "Driver Stopped!" << endl;

      scheduler.terminateAllTasks(schedulerDriver);
      schedulerDriver.stop();
      shutdown();

      if(schedulerDriver != NULL) {
         delete schedulerDriver;
         schedulerDriver = NULL;
      }
   }

   return status;
}


