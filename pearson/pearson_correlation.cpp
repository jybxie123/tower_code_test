#include "pearson_correlation.h"
#include <vector>
// #include <thread>
#include <future>
#include <pybind11/pybind11.h>
#include <pybind11/numpy.h>
#include <pybind11/stl.h> 
#include <mutex>
#include <algorithm>
#include <cmath>
#include <iostream>
#include <chrono> 

namespace py = pybind11;

std::vector<std::future<void>> futures;
std::mutex mtx;

struct PearsonResult {
    double sum_x = 0.0;
    double sum_y = 0.0;
    double sum_x_sq = 0.0;
    double sum_y_sq = 0.0;
    double sum_xy = 0.0;
    size_t count = 0;

    void merge(const PearsonResult& other) {
        sum_x += other.sum_x;
        sum_y += other.sum_y;
        sum_x_sq += other.sum_x_sq;
        sum_y_sq += other.sum_y_sq;
        sum_xy += other.sum_xy;
        count += other.count;
    }
};

void partial_pearson_correlation(const std::vector<double>& x, const std::vector<double>& y, PearsonResult& result, std::mutex& mtx, size_t start, size_t end, bool need_cal_x = true, bool need_cal_y = true) {
    PearsonResult partial_result;
    partial_result.count = end - start;
    for (size_t i = start; i < end; ++i) {
        if (need_cal_x){
            partial_result.sum_x += x[i];
            partial_result.sum_x_sq += x[i] * x[i];
        }
        if (need_cal_y){
            partial_result.sum_y += y[i];
            partial_result.sum_y_sq += y[i] * y[i];
        }
        partial_result.sum_xy += x[i] * y[i];
    }
    std::lock_guard<std::mutex> lock(mtx);
    result.merge(partial_result);
}

size_t get_available_threads() {
    return std::max(1u, std::thread::hardware_concurrency() / 2);
}

bool check_and_set_sum(double& sum, double res_value) {
    if (res_value > std::numeric_limits<double>::lowest()) {
        sum = res_value;

        return false;
    }
    return true;
}

double pearson_correlation(InterResult& res, const std::vector<double>&x, const std::vector<double>& y, size_t x_idx, size_t y_idx) {
    if (x.size()!= y.size()) {
        throw std::invalid_argument("Input vectors must have the same size.");
    }
    double sum_x = 0.0;
    double sum_y = 0.0;
    double sum_x_sq = 0.0;
    double sum_y_sq = 0.0;
    double sum_xy = 0.0;
    size_t n = x.size();
    bool need_cal_x = true;
    bool need_cal_y = true;
    {
        std::shared_lock<std::shared_mutex> lock(res.mtx);
        need_cal_x = check_and_set_sum(sum_x, res.column_sum[x_idx]) ||
            check_and_set_sum(sum_x_sq, res.column_sum_sq[x_idx]);
        need_cal_y = check_and_set_sum(sum_y, res.column_sum[y_idx]) ||
            check_and_set_sum(sum_y_sq, res.column_sum_sq[y_idx]);
    }
    for (size_t i = 0; i < n; ++i) {
        if (need_cal_x) {
            sum_x += x[i];
            sum_x_sq += x[i] * x[i];
        }
        if (need_cal_y) {
            sum_y += y[i];
            sum_y_sq += y[i] * y[i];
        }
        sum_xy += x[i] * y[i];
    }
    {
        std::unique_lock<std::shared_mutex> lock(res.mtx);
        if (need_cal_x) {
            res.column_sum[x_idx] = sum_x;
            res.column_sum_sq[x_idx] = sum_x_sq;
        }
        if (need_cal_y) {
            res.column_sum[y_idx] = sum_y;
            res.column_sum_sq[y_idx] = sum_y_sq;
        }
    }
    double numerator = sum_xy - (sum_x * sum_y / n);
    double denominator = std::sqrt((sum_x_sq - sum_x * sum_x / n) * (sum_y_sq - sum_y * sum_y / n));
    double pearson_correlation = (denominator == 0) ? 0 : numerator / denominator;

    return pearson_correlation;
}

double pearson_correlation_with_threads(InterResult& res, const std::vector<double>&x, const std::vector<double>& y, size_t x_idx, size_t y_idx) {
    if (x.size()!= y.size()) {
        throw std::invalid_argument("Input vectors must have the same size.");
    }
    size_t n_splits = get_available_threads();
    size_t split_size = x.size() / n_splits;
    PearsonResult result;
    bool need_sum_x = true;
    bool need_sum_y = true;
    {
        std::shared_lock<std::shared_mutex> lock(res.mtx);
        need_sum_x = check_and_set_sum(result.sum_x, res.column_sum[x_idx]) || 
            check_and_set_sum(result.sum_x_sq, res.column_sum_sq[x_idx]);
        need_sum_y = check_and_set_sum(result.sum_y, res.column_sum[y_idx]) ||
            check_and_set_sum(result.sum_y_sq, res.column_sum_sq[y_idx]);
    }
    for (size_t i = 0; i < n_splits; ++i) {
        size_t start_index = i * split_size;
        size_t end_index = (i == n_splits - 1) ? x.size() : (i + 1) * split_size;
        futures.push_back(std::async(std::launch::async, [&] {
            partial_pearson_correlation(std::ref(x), std::ref(y), std::ref(result), std::ref(mtx), start_index, end_index, need_sum_x, need_sum_y);
        }));
    }
    for (auto& fut : futures) {
        fut.get();
    }
    futures.clear();
    {   
        std::unique_lock<std::shared_mutex> lock(res.mtx); 
        res.column_sum[x_idx] = result.sum_x;
        res.column_sum[y_idx] = result.sum_y;
        res.column_sum_sq[x_idx] = result.sum_x_sq;
        res.column_sum_sq[y_idx] = result.sum_y_sq;
    }
    double numerator = result.sum_xy - (result.sum_x * result.sum_y / result.count);
    double denominator = std::sqrt((result.sum_x_sq - result.sum_x * result.sum_x / result.count) * (result.sum_y_sq - result.sum_y * result.sum_y / result.count));
    double pearson_correlation = (denominator == 0) ? 0 : numerator / denominator;

    return pearson_correlation;
}

std::vector<std::vector<double>> multiple_columns_pearson_correlation_with_threads(const std::vector<std::vector<double>>& columns) {
    size_t num_columns = columns.size();
    InterResult inter_res(num_columns);
    std::vector<std::vector<double>> results(num_columns, std::vector<double>(num_columns, 0.0));
    size_t num_threads = get_available_threads();
    size_t split_size = num_columns / num_threads;
    for (size_t i = 0; i < num_threads; ++i) {
        futures.push_back(std::async(std::launch::async, [&, i] {
            size_t end_index = (i == num_threads - 1) ? num_columns : (i + 1) * split_size;
            for (size_t k = i * split_size; k < end_index; ++k){
                for (size_t j = k+1; j < num_columns; ++j) {
                    double temp = pearson_correlation(inter_res, columns[k], columns[j], k, j);
                    results[k][j] = temp;
                    results[j][k] = temp;
                }
            }
        }));
    }
    for (auto& fut : futures) {
        fut.get(); 
    }
    futures.clear();
    for (size_t i = 0; i < num_columns; ++i) {
        results[i][i] = 1.0;
    }
    return results;
}

std::vector<std::vector<double>> multiple_columns_pearson_correlation(const std::vector<std::vector<double>>& columns) {
    size_t num_columns = columns.size();
    InterResult inter_res(num_columns);
    std::vector<std::vector<double>> results(num_columns, std::vector<double>(num_columns, 0.0));
    for( size_t i = 0; i < num_columns; ++i){
        for (size_t j = 0; j < num_columns; ++j){
            if(i!= j){
                std::vector<double> column_i = columns[i];
                std::vector<double> column_j = columns[j];
                double temp = pearson_correlation_with_threads(inter_res, column_i, column_j, i, j);
                results[i][j] = temp;
                results[j][i] = temp;
            }
            if(i == j){
                results[i][j] = 1.0; // Pearson correlation of a column with itself is always 1.0.
            }
        }
    }
    return results;
}


int main() {
    std::vector<std::vector<double>> columns = {
        {1.0, 2.0, 3.0},
        {4.0, 3.0, 6.0},
        {7.0, 8.0, 7.0}
    };

    auto start = std::chrono::high_resolution_clock::now();
    auto results = multiple_columns_pearson_correlation_with_threads(columns);
    for (const auto& row : results) {
        for (double val : row) {
            std::cout << val << " ";
        }
        std::cout << std::endl;
    }
    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> duration = end - start;
    std::cout<<"calculations took "<< duration.count() << " seconds" << std::endl;

    start = std::chrono::high_resolution_clock::now();
    results = multiple_columns_pearson_correlation(columns);
    for (const auto& row : results) {
        for (double val : row) {
            std::cout << val << " ";
        }
        std::cout << std::endl;
    }
    end = std::chrono::high_resolution_clock::now();
    duration = end - start;
    std::cout<<"calculations took "<< duration.count() << " seconds" << std::endl;
    // after test, the first one is much faster than the second one.
    return 0;
}

void fill_data(std::vector<std::vector<double>>& data, double* ptr, size_t rows, size_t cols, size_t start, size_t end) {
    for (size_t i = start; i < end; ++i) {
        for (size_t j = 0; j < cols; ++j) {
            data[j][i] = ptr[i * cols + j];
        }
    }
}

void fill_result(double *result_ptr, const std::vector<std::vector<double>>& result, size_t start, size_t end, size_t cols) {
    for (size_t i = start; i < end; ++i) {
        for (size_t j = 0; j < cols; ++j) {
            result_ptr[i * cols + j] = result[i][j];
        }
    }
}


PYBIND11_MODULE(pearson, m) {
    m.def("multiple_columns_pearson_correlation_with_threads", [](py::array_t<double> input) {
        py::buffer_info buf = input.request();
        if (buf.ndim != 2) {
            throw std::runtime_error("Input should be a 2D NumPy array");
        }

        size_t rows = buf.shape[0];
        size_t cols = buf.shape[1];
        std::vector<std::vector<double>> data(cols, std::vector<double>(rows));
        double *ptr = static_cast<double *>(buf.ptr);

        size_t num_threads = get_available_threads();
        size_t chunk_size = rows / num_threads;

        for (size_t t = 0; t < num_threads; ++t) {
            if (futures.size() >= num_threads) {
                for (auto& future : futures) {
                    future.get();
                }
                futures.clear();
            }
            size_t start = t * chunk_size;
            size_t end = (t == num_threads - 1) ? rows : start + chunk_size;
            futures.push_back(std::async(std::launch::async, fill_data, std::ref(data), ptr, rows, cols, start, end));
        }
        for (auto& future : futures) {
            future.get();
        }
        futures.clear();

        std::vector<std::vector<double>> result = multiple_columns_pearson_correlation_with_threads(data);
        py::array_t<double> py_result({result.size(), result[0].size()});
        auto result_buf = py_result.request();
        double *result_ptr = static_cast<double *>(result_buf.ptr);

        size_t result_rows = result.size();
        size_t result_cols = result[0].size();
        chunk_size = result_rows / num_threads;

        for (size_t t = 0; t < num_threads; ++t) {
            if (futures.size() >= num_threads) {
                for (auto& future : futures) {
                    future.get();
                }
                futures.clear();
            }
            size_t start = t * chunk_size;
            size_t end = (t == num_threads - 1) ? result_rows : start + chunk_size;
            futures.push_back(std::async(std::launch::async, fill_result, result_ptr, std::cref(result), start, end, result_cols));
        }

        for (auto& future : futures) {
            future.get();
        }
        futures.clear();
        return py_result;
    }, "Calculate Pearson correlation for multiple columns with threads");

    m.def("multiple_columns_pearson_correlation", [](py::array_t<double> input) {
        py::buffer_info buf = input.request();
        if (buf.ndim != 2) {
            throw std::runtime_error("Input should be a 2D NumPy array");
        }

        size_t rows = buf.shape[0];
        size_t cols = buf.shape[1];
        std::vector<std::vector<double>> data(cols, std::vector<double>(rows));
        double *ptr = static_cast<double *>(buf.ptr);

        size_t num_threads = get_available_threads();
        size_t chunk_size = rows / num_threads;

        for (size_t t = 0; t < num_threads; ++t) {
            if (futures.size() >= num_threads) {
                for (auto& future : futures) {
                    future.get();
                }
                futures.clear();
            }
            size_t start = t * chunk_size;
            size_t end = (t == num_threads - 1) ? rows : start + chunk_size;
            futures.push_back(std::async(std::launch::async, fill_data, std::ref(data), ptr, rows, cols, start, end));
        }
        for (auto& future : futures) {
            future.get();
        }
        futures.clear();

        std::vector<std::vector<double>> result = multiple_columns_pearson_correlation(data);
        py::array_t<double> py_result({result.size(), result[0].size()});
        auto result_buf = py_result.request();
        double *result_ptr = static_cast<double *>(result_buf.ptr);

        size_t result_rows = result.size();
        size_t result_cols = result[0].size();
        chunk_size = result_rows / num_threads;

        for (size_t t = 0; t < num_threads; ++t) {
            if (futures.size() >= num_threads) {
                for (auto& future : futures) {
                    future.get();
                }
                futures.clear();
            }
            size_t start = t * chunk_size;
            size_t end = (t == num_threads - 1) ? result_rows : start + chunk_size;
            futures.push_back(std::async(std::launch::async, fill_result, result_ptr, std::cref(result), start, end, result_cols));
        }
        for (auto& future : futures) {
            future.get();
        }
        futures.clear();
        return py_result;
    }, "Calculate Pearson correlation for multiple columns");
}


/*
我干了啥：
    1. 尽可能复用了中间结果，避免重复计算
    2. 在循环中尝试在两个地方增加了并行计算，并且比较效果。选择更好的一种。并行计算哪个更好，要考虑计算量，也要考虑cache miss的情况。
    3. 适度的考虑了一下cache miss的问题，但是知识浅薄，只能做到避免一些情况的cache miss，没办法保证尽到最好的cache命中率。
    TODO: 继续累加eigen库，看看哪个效果更好。
*/