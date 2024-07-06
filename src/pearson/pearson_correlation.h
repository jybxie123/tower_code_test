#ifndef PEARSON_CORRELATION_H
#define PEARSON_CORRELATION_H

#include <vector>
#include <shared_mutex>

struct InterResult {
    std::vector<double> column_sum;
    std::vector<double> column_sum_sq;
    mutable std::shared_mutex mtx;
    InterResult(size_t n) : column_sum(n, std::numeric_limits<double>::lowest()), column_sum_sq(n, std::numeric_limits<double>::lowest()) {}
};

double pearson_correlation(InterResult& res, const std::vector<double>&x, const std::vector<double>& y, size_t x_idx, size_t y_idx);

double pearson_correlation_with_threads(InterResult& res, const std::vector<double>&x, const std::vector<double>& y, size_t x_idx, size_t y_idx);

std::vector<std::vector<double> > multiple_columns_pearson_correlation_with_threads(const std::vector<std::vector<double> >& columns);

std::vector<std::vector<double> > multiple_columns_pearson_correlation(const std::vector<std::vector<double> >& columns);

#endif

