#include <pybind11/stl.h>
#include <pybind11/numpy.h>
#include <pybind11/pybind11.h>
#include <iostream>
#include <vector>
#include <future>
#include <map>
#include <string>
#include <chrono>
#include <ctime>
#include <iomanip>
using namespace std;
using namespace std::chrono;
namespace py = pybind11;


typedef map<time_t, map<string, double>> BetaMap;

double inf = std::numeric_limits<double>::lowest();
time_t day_sec = 24 * 60 * 60;
std::vector<std::future<void>> futures;
std::mutex mtx;
size_t max_threads = std::max(1u, std::thread::hardware_concurrency() / 2);

time_t stringToTimeT(const string& date) {
    struct tm tm = {};
    strptime(date.c_str(), "%Y-%m-%d", &tm);
    return mktime(&tm);
}

double longToDouble(time_t date) {
    return (double)date;
}


struct InterResult {
    vector<double> ticker_sum; // y
    vector<double> xy_sum;
    double market_sum = inf; // x
    double market_sq_sum = inf;
    InterResult(int length) : ticker_sum(length, inf), xy_sum(length, inf) {}
};

size_t max(size_t a, size_t b) {
    return a > b ? a : b;
}

BetaMap calculateBeta(const vector<time_t> times, const vector<vector<double>>& data, const vector<string>& tickers) { // data的第一维度是列数，也就是4，这样每一列都是一个vector，空间比较连续。
// data 包括tickers和market的数据，tickers是前面的列，market是最后一列。
    size_t ticker_num = tickers.size();
    BetaMap result;
    // const double* x = data[ticker_num].data();
    if (data.size() != ticker_num+1 or data.size() == 0){
        cerr << "No data provided." << endl;
        return result;
    }
    if (data[0].size() != times.size() or times.size() == 0) {
        cerr << "Time vector size does not match data vector size." << endl;
        return result;
    }
    size_t date_num = times.size();
    vector<size_t> start_dates(date_num);
    for (size_t i = 0; i < date_num; ++i) { 
        time_t date = times[i];
        time_t start_period = date - 128 * day_sec;
        start_period = (start_period > times[0]) ? start_period : times[0];
        for (size_t dt = max(i-200, 0); dt <= i; ++dt) { // 200 > 128*5/7
            if (start_period <= times[dt]) {
                start_dates[i] = dt;
                break;
            }
        }
    }
    size_t num_threads = max_threads;
    size_t chunk_size = date_num / num_threads;
    for (size_t th_i = 0; th_i < num_threads; ++th_i) {
        size_t start_idx = th_i * chunk_size;
        size_t end_idx = (th_i == num_threads - 1)? date_num : (th_i + 1) * chunk_size;
        if (futures.size() >= num_threads) {
            for (auto& future : futures) {
                future.get();
            }
            futures.clear();
        }
        futures.push_back(std::async(std::launch::async, [&, start_idx, end_idx] {
            InterResult res(ticker_num);
            for (size_t i = start_idx; i < end_idx; ++i) {
                time_t date = time_t(times[i]);
                if (res.market_sum == inf){
                    res.market_sum = 0;
                    res.market_sq_sum = 0;
                    for (size_t dt = start_dates[i]; dt < i; ++dt) { 
                        res.market_sum += data[ticker_num][dt];
                        res.market_sq_sum += data[ticker_num][dt] * data[ticker_num][dt];
                    }
                } else{
                    res.market_sum += data[ticker_num][i-1];
                    res.market_sq_sum += data[ticker_num][i-1] * data[ticker_num][i-1];
                    for (size_t dt = start_dates[i-1]; dt < start_dates[i]; ++dt){ 
                        res.market_sum -= data[ticker_num][dt];
                        res.market_sq_sum -= data[ticker_num][dt] * data[ticker_num][dt];
                    }
                }
                for (int tick = 0; tick < ticker_num; tick++) { 
                    if (res.ticker_sum[tick] == inf) { 
                        res.ticker_sum[tick] = 0;
                        res.xy_sum[tick] = 0;
                        for (size_t dt = start_dates[i]; dt < i; ++dt) {
                            res.ticker_sum[tick] += data[tick][dt];
                            res.xy_sum[tick] += data[tick][dt] * data[ticker_num][dt];
                        }
                    } else {
                        res.ticker_sum[tick] += data[tick][i-1];
                        res.xy_sum[tick] += data[tick][i-1] * data[ticker_num][i-1];       
                        for (size_t dt = start_dates[i-1]; dt < start_dates[i]; ++dt){
                            res.ticker_sum[tick] -= data[tick][dt];
                            res.xy_sum[tick] -= data[tick][dt] * data[ticker_num][dt];
                        }
                    }
                    double n = (i - start_dates[i]) > 0 ? (double)(i - start_dates[i]) : 1;
                    double nominator = res.xy_sum[tick] - ((res.ticker_sum[tick] * res.market_sum) / n);
                    double denominator = res.market_sq_sum - ((res.market_sum * res.market_sum) / n);
                    {
                        std::lock_guard<std::mutex> lock(mtx);
                        result[date][tickers[tick]] = (denominator != 0.0) ? nominator / denominator : 0.0;
                    }
                }
            }
        }));
    }
    for (auto& future : futures) {
        future.get();
    }
    futures.clear();
    return result;
}

int main() {
    vector<time_t> times = {stringToTimeT("2022-01-03"), stringToTimeT("2022-01-04"), stringToTimeT("2022-01-05")};
    vector<vector<double>> df = {
        {1.0, 2.0, 3.0},  // Ticker 1
        {1.5, 2.5, 3.5},  // Ticker 2
        {4.0, 6.0, 7.0}   // Market returns
    };

    vector<string> tickers = {"Ticker1", "Ticker2"};

    BetaMap beta = calculateBeta(times, df, tickers); 

    for (const auto& [date, ticker_beta] : beta) {
        cout << "Date: " << put_time(localtime(&date), "%Y-%m-%d") << endl;
        for (const auto& [ticker, beta_value] : ticker_beta) {
            cout << ticker << ": " << beta_value << endl;
        }
    }

    return 0;
}

std::tuple<std::vector<time_t>, std::vector<std::vector<double>>, std::vector<std::string>>
numpy_to_beta_input(py::array_t<double> np_array, std::vector<std::string> tickers) {
    py::buffer_info buf = np_array.request();
    if (buf.ndim != 2) {
        throw std::runtime_error("NumPy array must be two-dimensional");
    }

    size_t num_rows = buf.shape[0];
    size_t num_cols = buf.shape[1];

    if (num_cols != tickers.size() + 2) {  // Adjusted the number of columns check
        throw std::runtime_error("Number of columns in NumPy array does not match tickers size plus one");
    }

    auto ptr = static_cast<double*>(buf.ptr);

    std::vector<time_t> times(num_rows);
    std::vector<std::vector<double>> data(tickers.size() + 1, std::vector<double>(num_rows));

    size_t num_threads = max_threads;
    size_t chunk_size = num_rows / num_threads;
    for (size_t i = 0; i < num_threads; ++i) {
        size_t start_idx = i * chunk_size;
        size_t end_idx = (i == num_threads - 1) ? num_rows : (i + 1) * chunk_size;
        if (futures.size() >= num_threads) {
            for (auto& future : futures) {
                future.get();
            }
            futures.clear();
        }
        futures.push_back(std::async(std::launch::async, [&, start_idx, end_idx] {
            for (size_t idx = start_idx; idx < end_idx; ++idx) {
                times[idx] = static_cast<time_t>(ptr[idx]);  // Assuming the first column is Unix timestamp
                for (size_t j = 1; j < num_cols; ++j) {
                    data[j - 1][idx] = ptr[j * num_rows + idx];
                }
            }
        }));
    }
    for (auto& future : futures) {
        future.get();
    }
    futures.clear();
    return std::make_tuple(times, data, tickers);
}

py::array_t<double> convert_to_numpy(const BetaMap& beta_map, const std::vector<std::string>& tickers) {
    size_t num_dates = beta_map.size();
    size_t num_tickers = tickers.size();

    std::vector<double> result(num_dates * (num_tickers + 1));
    std::vector<time_t> dates;
    dates.reserve(num_dates);

    size_t num_threads = std::thread::hardware_concurrency();
    size_t chunk_size = num_dates / num_threads;

    for (size_t i = 0; i < num_threads; ++i) {
        if (futures.size() >= num_threads) {
            for (auto& future : futures) {
                future.get();
            }
            futures.clear();
        }
        size_t start = i * chunk_size;
        size_t end = (i == num_threads - 1) ? num_dates : (i + 1) * chunk_size;
        
        futures.push_back(std::async(std::launch::async, [&, start, end] {
            size_t ori_start = start;
            for (auto it = std::next(beta_map.begin(), start); it != std::next(beta_map.begin(), end); ++it) {
                const auto& [date, ticker_beta] = *it;
                dates.push_back(date);
                result[ori_start * (num_tickers + 1)] = static_cast<double>(date);
                for (size_t j = 0; j < num_tickers; ++j) {
                    result[ori_start * (num_tickers + 1) + j + 1] = ticker_beta.at(tickers[j]);
                }
                ++ori_start;
            }
        }));
    }

    for (auto& future : futures) {
        future.get();
    }
    futures.clear();
    py::array_t<double> result_array({num_dates, num_tickers + 1}, result.data());
    return result_array;
}

PYBIND11_MODULE(cal_beta, m) {
    m.def("cal_beta",
        [](py::array_t<double> np_array, std::vector<std::string> tickers) {
            auto [times, data, tickers_vec] = numpy_to_beta_input(np_array, tickers);
            // cout<< "print times"<<endl;
            // for (auto t: times){
            //     cout<<put_time(localtime(&t), "%Y-%m-%d") <<endl;
            // }
            BetaMap beta_map = calculateBeta(times, data, tickers_vec);
            return convert_to_numpy(beta_map, tickers_vec);
        },
        "A function that calculates beta values",
        py::arg("np_array"), py::arg("tickers"));
}


/*
修改两个传递函数，改为用time的一个单独列，一个data，一个ticker数组。
传回也是如此。单独时间列，data，ticker 数组。
看看为什么会段错误。
结果比对一下对不对
问题一也看看websocket是否符合预期。
*/