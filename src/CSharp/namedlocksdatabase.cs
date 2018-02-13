
using System;
using System.Data.SqlClient;


namespace hb {

	public delegate bool NamedMutexDBCallbackBreak(string reason);

	class NamedBusinessLockConnectionNames {
		public string Server;
		public string Database;
		public string User;
		public string Password;
	}

	class NamedBusinessLockConnection {
		public bool isOpen { get; private set; }
		static string createStatement = @"IF object_id('named_mutex') is null 
			BEGIN
			CREATE TABLE[named_mutex]([name] varchar(256) NOT NULL, [processid] varchar(256),
				[lockcount] int, [timeout] datetime2
				INDEX[named_mutex_ix1] NONCLUSTERED([name]));
			END";
		static NamedBusinessLockConnectionNames connectSettings;

		bool executeCommand(string command,out int rowsEffected) {
			string connetionString = null;
			rowsEffected = 0;
			SqlConnection cnn;
			connetionString = "Data Source="+ connectSettings.Server+";Initial Catalog="+
				connectSettings.Database+";User ID="+connectSettings.User+";Password="+
				connectSettings.Password;
			cnn = new SqlConnection(connetionString);
			try {
				cnn.Open();
				SqlCommand cmd = new SqlCommand(command, cnn);
				rowsEffected=cmd.ExecuteNonQuery();
				cnn.Close();
			}catch(Exception ex) {

			}
			return false;
		}

		public bool open(bool checkDatabaseExists = false) {
			return false;
		}
		bool close() {
			if (isOpen) {

			} else return false;
			return true;
		}
		bool reopen() {
			close();
			return open();
		}
		bool check_valid_database() {
			int rowsEffected;
			if (isOpen)
				return executeCommand(createStatement, out rowsEffected);
			return false;
		}

		public NamedBusinessLockConnection() { isOpen = false; }
		~NamedBusinessLockConnection() { close(); }
		public bool initializeDatabaseConnection(string server,string database,string user,string password) {
			connectSettings.Server = server;
			connectSettings.Database = database;
			connectSettings.User = user;
			connectSettings.Password = password;
			return true;
		}

	}




	public class NamedBusinessLockDatabase {
		private bool isAutoLocked=false;
		private NamedBusinessLockConnection connection=new NamedBusinessLockConnection();
		private bool check() {
			if (!connection.isOpen && !connection.open()) return false;
			return true;
		}
		#region public Interface
		public string Name { get; private set; }
		public string ProcessId { get; private set; }
		public int LastErrorCode { get; private set; }
		public const int NamedBusinessDefaultTimeOut = 120;
		public NamedBusinessLockDatabase(string name,string processId, bool autoLock=false) {
			isAutoLocked = autoLock;
		}
		~NamedBusinessLockDatabase() {

		}
		bool Lock(int waitForLockSec=NamedBusinessLockDatabase.NamedBusinessDefaultTimeOut,
			NamedMutexDBCallbackBreak callbackBreak=null,
			int timeoutSec=NamedBusinessLockDatabase.NamedBusinessDefaultTimeOut){
			return false;
		}
		bool Unlock() {
			return false;
		}
		bool TryLock(NamedMutexDBCallbackBreak callbackBreak = null,
			int timeoutSec = NamedBusinessLockDatabase.NamedBusinessDefaultTimeOut) {
			return false;
		}

		bool TimeLock(DateTime lockUntil, 
			int waitForLockSec = NamedBusinessLockDatabase.NamedBusinessDefaultTimeOut, 
			NamedMutexDBCallbackBreak callbackBreak = null,
			int timeoutSec = NamedBusinessLockDatabase.NamedBusinessDefaultTimeOut) {
			return false;
		}
		int RecursiveCount { get { return 0; } }
		bool IsLockedByMe
		{
			get { return false; }
		}
		bool IsLocked { get { return false; } }

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

