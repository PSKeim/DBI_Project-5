#ifndef STOPWATCH_H
#define STOPWATCH_H

#include <iostream>
#include <vector>
#include <time.h>


class StopWatch {

  public:

    StopWatch ();
    ~StopWatch ();

    void Start ();
    double CallTime ();
    double End ();

    double GetTime(int index);

  private:

    int clockRunning;
    int numberOfClicks;

    std::vector<double> times;
    time_t start;
    time_t stop;

    double CalculateTime (time_t begin, time_t end);

};
#endif
