// CPPSynchObjectsTest.cpp : Defines the entry point for the console application.
//

#include "stdafx.h"

#include "../../src/cpp/syncdbobjects.h"

#include <thread>
#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <iostream>
#include <mutex>

using namespace std;
using namespace hb;


wstring sql_server = L"localhost";
wstring sql_database = L"test";
wstring sql_user = L"dagobert";
wstring sql_password = L"topsecret";

vector<wstring> g_names;
vector<wstring> g_groups;

vector<thread*> g_threads;
mutex loglock;
int groupcount = 10;
int namecount = 10;
int loops = 10;

class PerformanceCounter {
private:
	__int64 laststart;
public:
	__int64 frequency;
	__int64 timer;

	PerformanceCounter(bool start = true) :timer(0), laststart(0) {
		::QueryPerformanceFrequency((LARGE_INTEGER*)&frequency);
		if (start) Start();
	}
	PerformanceCounter& Start() {
		::QueryPerformanceCounter((LARGE_INTEGER*)&laststart);
		return *this;
	}
	PerformanceCounter& Stop() {
		__int64 t = 0;
		::QueryPerformanceCounter((LARGE_INTEGER*)&t);
		if (laststart != 0) {
			timer += t - laststart;
			laststart = t;
		}
		return *this;
	}
	PerformanceCounter& Clear() {
		timer = laststart = 0;
		return *this;
	}
	int GetMilliseconds() {
		return (int)(timer * 1000 / frequency);
	}
	int GetMicroseconds() {
		return (int)(timer * 1000000 / frequency);
	}
};

void log(const string message) {
	loglock.lock();
	cout << message << endl;
	loglock.unlock();
}


wstring updatequery = L"execute updateSynchObject ? ";
wstring readquery = L"select syncpoint from syncobjects where namekey = ? ";
wstring groupquery = L"select namekey,syncpoint from syncobjects where namekey like ? ";

SyncObjectsStorage g_storage;



int main()
{
	g_storage.initialize(sql_server, sql_database, sql_user, sql_password);
	g_storage.addQueryBlock(L"q1", updatequery, readquery, groupquery);
	for (int g = 0; g < groupcount; g++) {
		g_groups.push_back(L"g" + to_wstring(g));
		for (int n = 0; n < namecount; n++) {
			wstring k = L"g" + to_wstring(g) + L"n" + to_wstring(n);
			g_names.push_back(k);
			auto obj = g_storage.syncObject(k, L"q1");
			if (!g_storage.updateDBObjectRemoteStatus(k))
				log("Error updateDBObject :" + to_string(g*namecount + n));
		}
	}
	Sleep(5000);
	for (int l = 0; l < loops; l++) {
		log("\nloop started");
		int isok = 0;
		PerformanceCounter perf;
		for (int i = 0, s = g_names.size(); i < s; i++) {
			g_storage.readDBObjectRemoteStatus(g_names[i]);
			auto obj = g_storage.syncObject(g_names[i]);
			if (obj.isValid())isok++;
		}
		perf.Stop();
		log("1 Step: Valid:" + to_string(isok)+ "  t:"+to_string(perf.GetMilliseconds())
			+" of "+to_string(g_storage.collectionSize()));
		isok = 0;
		perf.Clear().Start();
		for (int i = 0, s = g_groups.size(); i < s; i++) {
			wstring k = g_groups[i] + L"%" ;
			auto list= g_storage.readDBGroupRemoteStatus(k, L"q1");
			for (int k = 0, m = list.size(); k < m; k++) {
				auto t = list[k];
				if (get<1>(t))isok++;
			}
		}
		perf.Stop();
		log("2 Step: Valid:" + to_string(isok) + "  t:" + to_string(perf.GetMilliseconds()) 
			+ " of " + to_string(g_storage.collectionSize()));

		isok = 0;
		perf.Clear().Start();
		for (int i = 0, s = g_names.size(); i < s; i++) {
			if(!g_storage.updateDBObjectRemoteStatus(g_names[i]))
				log("Error updateDBObject :" + to_string(i));
			auto obj = g_storage.syncObject(g_names[i]);
			if (obj.isValid())isok++;
		}
		perf.Stop();
		log("3 Step: Valid:" + to_string(isok) + "  t:" + to_string(perf.GetMilliseconds())
			+ " of " + to_string(g_storage.collectionSize()));

		//Sleep(2000);

	}
    return 0;
}

