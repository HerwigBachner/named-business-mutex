// CPPNamedLocksTest.cpp : Defines the entry point for the console application.
//

#include "stdafx.h"
#include "tools.h"

#include <namedlocksprocess.h>
#include <namedlocksdatabase.h>
#include <namedlocks.h>
#include <thread>
#include <iostream>
#include <fstream>
#include <string>

using namespace std;
using namespace hb;

wstring sql_server = L"localhost";
wstring sql_database = L"test";
wstring sql_user = L"dagobert";
wstring sql_password = L"topsecret";



int g_names_count = 6;
int g_proc_count = 8;
vector<string> g_process_ids;
vector<string> g_names;
vector<thread*> g_threads;
vector<int>g_waits;
vector<int>g_failed;
int g_tries_succeed = 0;
int g_tries_failed = 0;
mutex loglock;
volatile int g_thread_finished = 0;

void log(const string message) {
	loglock.lock();
	cout << message << endl;
	loglock.unlock();
}
void cleanup_threads() {
	for (auto th : g_threads) {
		th->detach();
		delete(th);
	}
	g_thread_finished = 0;
	g_threads.clear();
}
void basetestoutput(int process_id, bool isError, string message) {
	loglock.lock();
	if (isError) cout << "ERROR ";
	cout << boost::posix_time::to_iso_extended_string(
		boost::posix_time::microsec_clock::universal_time()) << " ";
	cout << "Thread " << g_process_ids[process_id] << " " << message;
	cout << endl;
	loglock.unlock();
}
int initbasetestworker(std::function<void(int)> func, int count) {
	int th = 0;
	g_thread_finished = 0;
	g_threads.clear();
	while (count--) g_threads.push_back(new thread(func, ++th));
	return th;
}

template <class T>
void basetestworker1(int process_id) {
	Named_Business_Lock<T> nbl(g_names[0], g_process_ids[process_id], false);
	basetestoutput(process_id, false, "started");
	basetestoutput(process_id, false, "is locked by me ? " + string(nbl.is_locked_by_me() ? "TRUE" : "FALSE"));
	if (nbl.lock()) {
		basetestoutput(process_id, false, "locked");
		::Sleep(1000);
		basetestoutput(process_id, false, "is locked by me ? " + string(nbl.is_locked_by_me() ? "TRUE" : "FALSE"));
		::Sleep(2000);
		if (nbl.unlock()) {
			basetestoutput(process_id, false, "unlocked");
		} else {
			basetestoutput(process_id, true, "Could not unlock");
		}
	} else {
		basetestoutput(process_id, true, "Could not lock");
	}
	basetestoutput(process_id, false, "ended");
	g_thread_finished++;
}

template <class T>
void basetestworker2(int process_id) {
	Named_Business_Lock<T> nbl(g_names[0], g_process_ids[process_id], false);
	basetestoutput(process_id, false, "started");
	while (!nbl.trylock()) {
		basetestoutput(process_id, false, "trylock FAILED");
		::Sleep(300);
	}
	basetestoutput(process_id, false, "locked");
	::Sleep(3000);
	if (nbl.unlock()) {
		basetestoutput(process_id, false, "unlocked");
	} else {
		basetestoutput(process_id, true, "Could not unlock");
	}
	basetestoutput(process_id, false, "ended");
	g_thread_finished++;
}


template <class T>
void basetest1(int count = 2) {
	cout << endl << "Base test - simple lock - started" << endl;
	int th = initbasetestworker(basetestworker1<T>, count);
	while (g_thread_finished < th) ::Sleep(500);
	cleanup_threads();
	cout << "Base test 1 ended" << endl << endl;
}

template <class T>
void basetest2(int count = 2) {
	cout << endl << "Base test - try lock - started" << endl;
	int th = initbasetestworker(basetestworker2<T>, count);
	while (g_thread_finished < th) ::Sleep(500);
	cleanup_threads();
	cout << "Base test 2 ended" << endl << endl;
}
template <class T>
void basetestworker3(int process_id) {
	Named_Business_Lock<T> nbl(g_names[0], g_process_ids[process_id], false);
	bool doit = true;
	basetestoutput(process_id, false, "started");
	boost::posix_time::ptime timeout = boost::posix_time::microsec_clock::universal_time() +
		boost::posix_time::time_duration(0, 2, 0);
	while (doit) {
		boost::posix_time::ptime until = boost::posix_time::microsec_clock::universal_time() +
			boost::posix_time::time_duration(0, 0, 3);
		if (nbl.time_lock(until) && nbl.is_locked_by_me() && 
			until > boost::posix_time::microsec_clock::universal_time()) {
			basetestoutput(process_id, false, " locked until " +
				boost::posix_time::to_iso_extended_string(until));
			doit = false;
		} else {
			basetestoutput(process_id, true, "Could not lock");
			::Sleep(300);
		}
		if (boost::posix_time::microsec_clock::universal_time() > timeout) doit = false;
	}
	basetestoutput(process_id, false, "ended");
	g_thread_finished++;
}

template <class T>
void basetest3(int count = 2) {
	cout << endl << "Base test - timelock lock - started" << endl;
	int th = initbasetestworker(basetestworker3<T>, count);
	while (g_thread_finished < th) ::Sleep(500);
	cleanup_threads();
	cout << "Base test 3 ended" << endl << endl;
}

int main()
{
	cout << "Named Mutex Tester Version 1 Herwig Bachner (c) 2018" << endl;
	for (int i = 0; i < g_names_count; i++) {
		g_names.push_back("Name_" + to_string(i));
	}
	for (int i = 0; i < g_proc_count; i++) {
		g_process_ids.push_back("Process_" + to_string(i));
		g_waits.push_back(0);
		g_failed.push_back(0);
	}
	cout << "1.Test: using " + to_string(g_names_count) + " named locks - Threads and Process Id: "
		+ to_string(g_proc_count) << endl << endl;
	PerformanceCounter perf1;
	basetest1<NamedBusiness_Lock_ProcessItem>();
	basetest2<NamedBusiness_Lock_ProcessItem>();
	basetest3<NamedBusiness_Lock_ProcessItem>();

	cout << "Test with Process Locks needed " << perf1.Stop().GetMilliseconds() << endl;

	if (Initialize_NamedBusinessLocks_For_Database(sql_server, sql_database, sql_user,sql_password, true)) {
		cout << "Database Mutex OK" << endl;
		PerformanceCounter perf2;
		basetest1<NamedBusiness_Lock_DatabaseItem>(3);
		basetest2<NamedBusiness_Lock_DatabaseItem>(3);
		basetest3<NamedBusiness_Lock_DatabaseItem>(3);
		cout << "Test with Database Locks needed " << perf2.Stop().GetMilliseconds() << endl;
	}


	cout << "Named Mutex Tester finished" << endl;

    return 0;
}

