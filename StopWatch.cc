#include "StopWatch.h"
#include <iostream>
#include <vector>
#include <time.h>

/**
 * Constructor
 */
StopWatch::StopWatch (){

    // Clock is not running right now
  clockRunning = 0;

    // Zero out start and stop variables
  start = (time_t) 0.0;
  stop = (time_t) 0.0;

    // No records right now
  numberOfClicks = 0;

} // end StopWatch::StopWatch


/**
 * Destructor
 */
StopWatch::~StopWatch (){

}

/**
 * Starts the stopwatch.
 */
void StopWatch::Start(){

  if (!clockRunning){

      // Set the flag
    clockRunning = 1;
      // Get the current time
    start = time(NULL);
      // Zero out the stop time
    stop = (time_t) 0;

  } // end if !clockRunning

} // end StopWatch::Start

/**
 * Calculates the first time period. The time
 * period is measured from a previous call
 * to CallTime() or from Start().  The real
 * world analogy is of a coach timing the laps
 * of a runner.
 *
 * @return
 *    A double representing the length of time
 *    elapsed.
 */
double StopWatch::CallTime(){

    // Only do something if the clock is active
  if (clockRunning){

      // Get the current time and add result to the vector
    time_t click = time(NULL);
    times.push_back(CalculateTime(start,click));

      // Reset start and increment number of measurements
    start = click;
    numberOfClicks++;

  } // end if clockRunning

} // end StopWatch::CallTime

/**
 * Ends the running of the clock. The time
 * is saved and the clock counter stops.
 *
 * @return
 *    A double representing the length of time
 *    elapsed.    
 */
double StopWatch::End(){

    // Only execute if clock is running
  if (clockRunning){

      // Stop the clock
    clockRunning = 0;

      // Get the time elapsed
    stop = time(NULL);
    times.push_back(CalculateTime(start,stop));

      // Increment number of measurements
    numberOfClicks++;

  } // end if clockRunning


} // end StopWatch::End


/**
 * Get the time for a specific recording.
 *
 * @param index
 *    The specific measurement to get. Must be
 *    an int in the the range 0 <= index <= numberOfClicks.
 *
 * @return
 *    If index >= numberOfClicks, 0 is returned. Else,
 *    a double > 0 is returned, representing the number of
 *    seconds passed for the event.
 */
double StopWatch::GetTime (int index){

  double time = 0;

    //
  if (index < numberOfClicks){
    time = times[index];
  }

  return time;

} // end StopWatch::GetTime

/**
 * Given two time_t variables, calculate the
 * seconds elapsed as a double.
 *
 * @param begin
 *    The starting time_t value.
 * @param end
 *    The ending time_t value.
 *
 * @return
 *    The elapsed time, in seconds.
 */
double StopWatch::CalculateTime (time_t begin, time_t end){
  return (double) end - (double) begin;
} // end StopWatch::CalculateTime
