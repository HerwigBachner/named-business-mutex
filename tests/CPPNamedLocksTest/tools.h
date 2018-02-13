#pragma once
// windows.h needed to be included

namespace hb {
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
};

