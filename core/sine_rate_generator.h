#ifndef YCSB_C_SINE_RATE_GENERATOR_H_
#define YCSB_C_SINE_RATE_GENERATOR_H_

#include "generator.h"
#include "timer.h"
#include <cmath>
#include <mutex>
#include <ratio>

namespace ycsbc {
class SineRateGenerator : public Generator<uint64_t> {
private:
  double sine_a_;
  double sine_b_;
  double sine_c_;
  double sine_d_;
  uint64_t sine_mix_rate_interval_microseconds_;
  std::mutex mu_;
  int period_count_;
  int period_total_req_;
  int period_req_count_;
  uint64_t interval_us_in_period_;
  uint64_t base_time_us_;

public:
  SineRateGenerator(double sine_a, double sine_b, double sine_c, double sine_d,
                    int sine_mix_rate_interval_milliseconds)
      : sine_a_(sine_a), sine_b_(sine_b), sine_c_(sine_c), sine_d_(sine_d),
        sine_mix_rate_interval_microseconds_(
            uint64_t(sine_mix_rate_interval_milliseconds) * 1000),
        period_count_(0), period_total_req_(0), period_req_count_(0),
        interval_us_in_period_(0), base_time_us_(get_now_micros()) {
    {
      period_total_req_ = CalcPeroidReqCount(
          double(base_time_us_ +
                 period_count_ * sine_mix_rate_interval_microseconds_) /
          double(std::micro::den));
      interval_us_in_period_ =
          sine_mix_rate_interval_microseconds_ / period_total_req_;
    };
  }

  uint64_t Next() override {
    std::lock_guard<std::mutex> lock(mu_);
    if (++period_req_count_ == period_total_req_) {
      period_count_++;
      period_req_count_ = 0;
      period_total_req_ = CalcPeroidReqCount(
          double(base_time_us_ +
                 period_count_ * sine_mix_rate_interval_microseconds_) /
          double(std::micro::den));

      interval_us_in_period_ =
          sine_mix_rate_interval_microseconds_ / period_total_req_;
    }

    return CalcReqSendMicros();
  }

  uint64_t Last() override {
    std::lock_guard<std::mutex> lock(mu_);
    return CalcReqSendMicros();
  }

private:
  double CalcQPS(double second) {
    return sine_a_ * std::sin(sine_b_ * second + sine_c_) + sine_d_;
  }

  uint64_t CalcPeroidReqCount(double second) {
    return uint64_t(CalcQPS(second) *
                    (double(sine_mix_rate_interval_microseconds_) /
                     double(std::micro::den)));
  }

  uint64_t CalcReqSendMicros() {
    return base_time_us_ +
           period_count_ * sine_mix_rate_interval_microseconds_ +
           period_req_count_ * interval_us_in_period_;
  }
};
} // namespace ycsbc

#endif // YCSB_C_SINE_RATE_GENERATOR_H_