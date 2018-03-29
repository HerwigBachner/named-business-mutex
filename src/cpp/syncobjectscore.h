#ifndef HB_TOOLS_SYNC_CORE_OBJECTS
#define HB_TOOLS_SYNC_CORE_OBJECTS
#include <string>
#include <unordered_map>

namespace hb {
	using namespace std;
	using namespace boost::posix_time;


	template <typename T>
	class Sync_Object {
	protected:
		T m_remote;
		T m_local;
		wstring m_key;
	public:
		Sync_Object(const wstring& k) : m_key(k) {}
		Sync_Object(const wstring& k, const T &x) :m_key(k), m_local(x), m_remote(x) {}
		Sync_Object(const T &x) :m_local(x), m_remote(x) {}
		void remoteAndLocal(T& t) { m_local = m_remote = t; }
		void local(T& t) { m_local = t; }
		void remote(T &t) { m_remote = t; }
		const T& local()const { return m_local; }
		const T& remote()const { return m_remote; }
		const wstring& key()const { return m_key; }
		bool isValid()const { return m_remote <= m_local; }
		virtual void resetToDefault() {}
		operator bool() { return isValid(); }
	};


	template<typename T>
	class SyncMap :public unordered_map<wstring, T*> {
	public:
		~SyncMap() {
			for (auto it = this->begin(); it != this->end(); ++it)delete it->second;
		}
		T* get(const wstring& key) {
			auto it = find(key);
			return (it != end()) ? it->second : nullptr;
		}
	};


	template<typename T>
	class SyncObjectStorageBase {
	protected:
		SyncMap<T> m_global_list;
	public:

		bool isObjectValid(const wstring& key) {
			T* result = m_global_list.get(key);
			return result ? result->isValid() : false;
		}
		size_t collectionSize() {
			return m_global_list.size();
		}
		T* getObject(const wstring& key, bool* isnew = nullptr) {
			T* result = m_global_list.get(key);
			if (!result && isnew && *isnew) {
				result = new T(key);
				result->resetToDefault();
				m_global_list.insert(make_pair(key, result));
			} else if (isnew) *isnew = false;
			return result;
		}
	};

}


#endif