
using System;
using System.Data.SqlClient;
using System.Threading;


namespace hb {

	public delegate bool NamedMutexDBCallbackBreak(string reason);

	class NamedBusinessLockConnectionNames {
		public string Server;
		public string Database;
		public string User;
		public string Password;
	}

	class NamedBusinessLockConnection {
		SqlConnection connection = null;
		public bool isOpen { get; private set; }
		static string createStatement = @"IF object_id('named_mutex') is null 
			BEGIN
			CREATE TABLE[named_mutex]([name] varchar(256) NOT NULL, [processid] varchar(256),
				[lockcount] int, [timeout] datetime2
				INDEX[named_mutex_ix1] NONCLUSTERED([name]));
			END";

		static string readStatement = @"select processid,lockcount,timeout from named_mutex where 
				name=@name";
		static string insertStatement = @"insert into named_mutex(name,processid,lockcount,timeout) 
				values(@name,@processid,@lockcount,@timeout) ";
		static string incrementStatement = @"update named_mutex set processid=@processid, lockcount=
			IIF((lockcount < @lockcount) or (timeout< GETUTCDATE()),@lockcount,lockcount+1), timeout=@timeout 
			where name=@name and ((lockcount <= 0) or (processid=@processid) or (timeout< GETUTCDATE())) ";
		static string decrementStatement = @"update named_mutex set lockcount=IIF(lockcount <= 0,0,
			lockcount-1), processid=IIF(lockcount=1,'',processid) where name=@name and 
			(lockcount > 0) and (processid=@processid) ";

		static NamedBusinessLockConnectionNames connectSettings;



		bool executeCommand(string command, out int rowsEffected) {
			bool result = false;
			rowsEffected = 0;
			if (!isOpen) return result;
			try {
				SqlCommand cmd = new SqlCommand(command, connection);
				rowsEffected = cmd.ExecuteNonQuery();
				result = true;
			} catch (Exception ex) {

			}
			return false;
		}
		public bool check_valid_database() {
			int rowsEffected;
			if (isOpen)
				return executeCommand(createStatement, out rowsEffected);
			return false;
		}

		bool createData(string name, string processId, int timeOutSec) {
			bool result = false;
			if (isOpen) {
				try {
					SqlCommand cmd = new SqlCommand(insertStatement, connection);
					cmd.Parameters.Add(new SqlParameter("@name", name));
					cmd.Parameters.Add(new SqlParameter("@processid", processId));
					cmd.Parameters.Add(new SqlParameter("@lockcount", 0));
					cmd.Parameters.Add(new SqlParameter("@timeout", DateTime.UtcNow.AddSeconds(timeOutSec)));
					result = cmd.ExecuteNonQuery() == 1;
				} catch (Exception ex) {

				}
			}
			return result;
		}
		bool readData(string name, ref string processId, ref int lockCount, ref DateTime timeout,
			bool checknew = false) {
			bool result = false;
			if (isOpen) {
				try {
					SqlCommand cmd = new SqlCommand(readStatement, connection);
					cmd.Parameters.Add(new SqlParameter("@name", name));
					using (SqlDataReader reader = cmd.ExecuteReader()) {
						if (reader.HasRows) {
							if (result = reader.Read()) {
								processId = reader.GetString(0);
								lockCount = reader.GetInt32(1);
								timeout = reader.GetDateTime(2);
							}
						} else if (checknew) {
							result = createData(name, processId, 0);
						}
					}
				} catch (Exception ex) {

				}
			}
			return result;
		}
		bool incrementData(string name, string processId, int lockCount, DateTime timeout) {
			bool result = false;
			if (isOpen) {
				try {
					SqlCommand cmd = new SqlCommand(incrementStatement, connection);
					cmd.Parameters.Add(new SqlParameter("@name", name));
					cmd.Parameters.Add(new SqlParameter("@processid", processId));
					cmd.Parameters.Add(new SqlParameter("@lockcount", lockCount));
					cmd.Parameters.Add(new SqlParameter("@timeout", timeout));
					result = cmd.ExecuteNonQuery() == 1;
				} catch (Exception ex) {

				}
			}
			return result;
		}
		bool decrementData(string name) {
			bool result = false;
			if (isOpen) {
				try {
					SqlCommand cmd = new SqlCommand(decrementStatement, connection);
					cmd.Parameters.Add(new SqlParameter("@name", name));
					result = cmd.ExecuteNonQuery() == 1;
				} catch (Exception ex) {

				}
			}
			return result;
		}

		#region Interface
		public bool open(bool checkDatabaseExists = false) {
			bool result = false;
			if (!isOpen) {
				string connectString = "Data Source=" + connectSettings.Server + ";Initial Catalog=" +
					connectSettings.Database + ";User ID=" + connectSettings.User + ";Password=" +
					connectSettings.Password;
				connection = new SqlConnection(connectString);
				try {
					connection.Open();
					isOpen = true;
					result = true;
				} catch (Exception ex) {
					connection = null;
				}
			}
			if (checkDatabaseExists) result = check_valid_database();
			return result;
		}
		public bool close() {
			if (isOpen) {
				connection = null;
				isOpen = false;
			} else return false;
			return true;
		}
		public bool reopen() {
			close();
			return open();
		}
		public bool decrement(string name,string processId) {
			bool result = false;
			try {
				string mProcessId="";
				int mLockCount = 0;
				DateTime mTimeout = DateTime.UtcNow;
				if (readData(name,ref mProcessId,ref mLockCount,ref mTimeout) && 
					(mProcessId == processId) && (mLockCount > 0))
					result = decrementData(name);
			} catch (Exception ex) {
			}
			return result;
		}
		public bool increment(string name, string processId,ref int lockCount, DateTime timeout) {
			bool result = false;
			try {
				string mProcessId = "";
				DateTime mTimeout = DateTime.UtcNow;
				int mLockCount = 0;
				if (readData(name, ref mProcessId, ref mLockCount, ref mTimeout, true)) {
					if ((mLockCount <= 0) || mTimeout> DateTime.UtcNow) {
						lockCount = 1;
						result = incrementData(name,processId,lockCount,timeout);
					} else if (mProcessId == processId) {
						lockCount++;
						result = incrementData(name, processId, lockCount,timeout);
					}
				}
			} catch (Exception ex) {
			}
			return result;
		}

		public NamedBusinessLockConnection() { isOpen = false; }
		~NamedBusinessLockConnection() { close(); }
		#endregion
		#region Statics
		public static bool initializeDatabaseConnection(string server, string database, string user, string password) {
			connectSettings = new NamedBusinessLockConnectionNames();
			connectSettings.Server = server;
			connectSettings.Database = database;
			connectSettings.User = user;
			connectSettings.Password = password;
			return true;
		}
		#endregion
	}




	public class NamedBusinessLockDatabase {
		private bool isAutoLocked = false;
		private NamedBusinessLockConnection connection = new NamedBusinessLockConnection();
		private bool check() {
			if (!connection.isOpen && !connection.open()) return false;
			return true;
		}
		#region internal interface

		bool increment( DateTime timeout) {
			return connection.increment(Name, ProcessId, ref LockCount, timeout);
		}


		int _lock( int waitForLockSec, int timeoutSec) {
			int result = -1;
			if (!check()) return result;
			for (int t = 0, tout = waitForLockSec * 1000; t < tout; t += 10) {
				if ((result = _tryLock(timeoutSec)) > 0) break;
				Thread.Sleep(10);
			}
			return result;
		}
		int _unlock() {
			int result = -1;
			if (!check()) return result;
			return connection.decrement(Name,ProcessId) ? 1 : -1;
		}
		int _tryLock( DateTime timeout) {
			int result = -1;
			if (!check()) return result;
			return increment(timeout) ? 1 : -1;
		}

		int _tryLock(int timeoutSec) {
			return _tryLock(DateTime.UtcNow.AddSeconds(timeoutSec));
		}
		int _timeLock( DateTime timeout, int waitForLockSec) {
			int result = -1;
			if (!check()) return result;
			for (int t = 0, tout = waitForLockSec * 1000; t < tout; t += 10) {
				if ((result = _tryLock( timeout)) > 0)
					break;
				Thread.Sleep(10);
			}
			return result;
		}
		#endregion

		#region public Interface
		public string Name { get; private set; }
		public string ProcessId { get; private set; }
		public int LastErrorCode { get; private set; }
		int LockCount;
		public const int NamedBusinessDefaultTimeOut = 120;
		public NamedBusinessLockDatabase(string name, string processId, bool autoLock = false) {
			isAutoLocked = autoLock;
		}
		~NamedBusinessLockDatabase() {

		}
		bool Lock(int waitForLockSec = NamedBusinessLockDatabase.NamedBusinessDefaultTimeOut,
			NamedMutexDBCallbackBreak callbackBreak = null,
			int timeoutSec = NamedBusinessLockDatabase.NamedBusinessDefaultTimeOut) {
			return _lock(waitForLockSec,timeoutSec)>0;
		}
		bool Unlock() {
			return _unlock() > 0;
		}
		bool TryLock(NamedMutexDBCallbackBreak callbackBreak = null,
			int timeoutSec = NamedBusinessLockDatabase.NamedBusinessDefaultTimeOut) {
			return  _tryLock(timeoutSec)>0;
		}

		bool TimeLock(DateTime lockUntil,
			int waitForLockSec = NamedBusinessLockDatabase.NamedBusinessDefaultTimeOut,
			NamedMutexDBCallbackBreak callbackBreak = null) {
			return _timeLock(lockUntil,waitForLockSec)>0;
		}
		int RecursiveCount { get { return LockCount; } }
		bool IsLockedByMe
		{
			get { return false; }
		}
		bool IsLocked { get { return LockCount >= 0; } }

		string LockOwnedProcessId { get { return ""; } }
		bool Reset() {
			return false;
		}
		bool Remove() {
			return false;
		}
		#endregion
	}

}

