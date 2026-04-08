#ifndef COMMUNICATOR_TOOL_PACKING
#define COMMUNICATOR_TOOL_PACKING

/// @brief Hedgehog namespace
namespace hh {
/// @brief Communicator namespace
namespace comm {
/// @brief Tool namespace
namespace tool {

/// @brief Write data in a buffer of bytes.
/// @tparam T Type of the data.
/// @param buf  Buffer of bytes to write to.
/// @param data Data to write.
/// @return Size of the buffer after the write.
template <typename T>
size_t writeBytes(std::vector<char> &buf, T const &data) {
  size_t pos = buf.size();
  buf.resize(pos + sizeof(T));
  std::memcpy(&buf[pos], &data, sizeof(T));
  return buf.size();
}

/// @brief Write a vector of data to a buffer of bytes.
/// @tparam T Type of the data.
/// @param buf  Buffer of bytes to write to.
/// @param data  Data to write.
/// @param count Number of elements to write.
/// @return Size of the buffer after the write.
template <typename T>
size_t writeBytes(std::vector<char> &buf, T *data, size_t count) {
  assert(data != nullptr);
  size_t pos = buf.size();
  buf.resize(pos + sizeof(T) * count);
  std::memcpy(&buf[pos], data, sizeof(T) * count);
  return buf.size();
}

/// @brief Read some bytes into the given data starting at pos.
/// @tparam T Type of the data.
/// @param buf  Buffer to read.
/// @param pos  Position in the buffer to start reading.
/// @param data Data to read into.
/// @return Position of the next data in the buffer.
template <typename T>
size_t readBytes(std::vector<char> const &buf, size_t pos, T &data) {
  assert(pos + sizeof(T) <= buf.size());
  memcpy(&data, &buf[pos], sizeof(T));
  return pos + sizeof(T);
}

/// @brief Read count elements into the given buffer starting at pos.
/// @tparam T Type of the data.
/// @param buf  Buffer to read.
/// @param pos  Position in the buffer to start reading.
/// @param data  Data to read.
/// @param count Number of elements to read.
/// @return Position of the next data in the buffer.
template <typename T>
size_t readBytes(std::vector<char> const &buf, size_t pos, T *data, size_t count) {
  assert(data != nullptr);
  assert(pos + count * sizeof(T) <= buf.size());
  std::memcpy(data, &buf[pos], sizeof(T) * count);
  return pos + sizeof(T) * count;
}

} // end namespace tool

} // end namespace comm

} // end namespace hh

#endif
