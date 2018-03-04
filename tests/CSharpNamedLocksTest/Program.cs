using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using hb;

namespace CSharpNamedLocksTest {

	class Test1 {

		public int gCount = 10;
		public static string[] gNames;
		public static string[] gProcessIds;
		private static Mutex mutex = new Mutex();
		private static int counter;

		string sqlServer = "localhost";
		string sqlDatabase = "test";
		string sqlUser = "dagobert";
		string sqlPassword = "topsecret";

		public Test1() {
			gNames = new string[gCount];
			gProcessIds = new string[gCount];
			for(int i=0;i< gCount; i++) {
				gNames[i] = "Name_" + i.ToString();
				gProcessIds[i] = "Process_" + i.ToString();
			}
		}

		static void output(string what) {
			mutex.WaitOne();
			Console.WriteLine(DateTime.UtcNow.ToLongTimeString()+" : "+what);
			mutex.ReleaseMutex();
		}



		static void basetestworker2(object idobj) {
			int id = (int)idobj;
			NamedBusinessLockDatabase nbl = new NamedBusinessLockDatabase(gNames[0], gProcessIds[id], false);
			output(gProcessIds[id] + " started");
			while (!nbl.TryLock()) {
				output(gProcessIds[id] + " trylock FAILED");
				Thread.Sleep(300);
			}
			output(gProcessIds[id] + " locked");
			Thread.Sleep(3000);
			if (nbl.Unlock()) {
				output(gProcessIds[id] + " unlocked");
			} else {
				output(gProcessIds[id] + " Could not unlock: " + nbl.LastErrorMessage);
			}
			output(gProcessIds[id] + " ended");
			counter--;
		}

		static void basetestworker3(object idobj) {
			int id = (int)idobj;
			NamedBusinessLockDatabase nbl = new NamedBusinessLockDatabase(gNames[0], gProcessIds[id], false);
			output(gProcessIds[id] + " Test 3 started");
			bool doit = true;
			DateTime timeout = DateTime.UtcNow + new TimeSpan(0, 2, 0);
			while (doit) {
				DateTime until = DateTime.UtcNow + new TimeSpan(0, 0, 3);
				if (nbl.TimeLock(until) && nbl.IsLockedByMe && until>DateTime.UtcNow) {
					output(gProcessIds[id] + " locked until " + until.ToLongTimeString());
					doit = false;
				} else {
					output(gProcessIds[id] + " Could not lock: " + nbl.LastErrorMessage);
					Thread.Sleep(300);
				}
				if (DateTime.UtcNow > timeout) doit = false;
			}
			output(gProcessIds[id] + " ended");
			counter--;
		}

		static void basetestworker1(object idobj) {
			int id = (int)idobj;
			NamedBusinessLockDatabase nbl = new NamedBusinessLockDatabase(gNames[0], gProcessIds[id], false);
			output(gProcessIds[id] + " started");
			output(gProcessIds[id] + " is locked by me? "+ (nbl.IsLockedByMe ? "TRUE" : "FALSE"));
			if (nbl.Lock()) {
				output(gProcessIds[id] + " locked");
				Thread.Sleep(1000);
				output(gProcessIds[id] + " is locked by me ? " + (nbl.IsLockedByMe ? "TRUE " : "FALSE ")+nbl.RecursiveCount.ToString());
				Thread.Sleep(2000);
				if (nbl.Unlock()) {
					output(gProcessIds[id] + " unlocked");
				} else {
					output(gProcessIds[id] + " Could not unlock: " + nbl.LastErrorMessage);
				}
			} else {
				output(gProcessIds[id] + " Could not lock: " + nbl.LastErrorMessage);
			}
			output(gProcessIds[id] + " ended");
			counter--;

		}

		void basetest1(int count) {
			Console.WriteLine("Base test - simple lock - started");
			counter = count;
			for(int i = 0; i < count; i++) {
				new Thread(basetestworker1).Start(i);
			}
			while (counter > 0) Thread.Sleep(500);

		}
		void basetest2(int count) {
			Console.WriteLine("Base test - try lock - started");
			counter = count;
			for (int i = 0; i < count; i++) {
				new Thread(basetestworker2).Start(i);
			}
			while (counter > 0) Thread.Sleep(500);

		}
		void basetest3(int count) {
			Console.WriteLine("Base test - time lock - started");
			counter = count;
			for (int i = 0; i < count; i++) {
				new Thread(basetestworker3).Start(i);
			}
			while (counter > 0) Thread.Sleep(500);

		}

		public bool Execute(int count) {
			Console.WriteLine("1.Test: using " + gCount.ToString() +
				" named locks - Threads and Process Ids: " + count.ToString());
			if (hb.NamedBusinessLockConnection.initializeDatabaseConnection(sqlServer, sqlDatabase, sqlUser, sqlPassword)) {
				Console.WriteLine("database opened!!");
				basetest1(3);
				basetest2(3);
				basetest3(3);
				return true;
			}
			return false;
		}
	}

	class Program {
		static void Main(string[] args) {
			Console.WriteLine("Named Mutex Tester Version 1 Herwig Bachner (c) 2018");
			Test1 t1 = new Test1();
			t1.Execute(3);
		}
	}
}
