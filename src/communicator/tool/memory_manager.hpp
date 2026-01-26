#ifndef COMMUNICATOR_TOOL_MEMORY_MANAGER
#define COMMUNICATOR_TOOL_MEMORY_MANAGER
#include "../type_map.hpp"
#include <memory>
#include <source_location>
#include <type_traits>

/// The memory manager is used to allocated memory on the receiver end of the
/// communicator. It can also be used freely to control the amount of memory
/// allocated by a program.
///
/// A memory manager should implement the following functions:
/// - allocate(mode, location)
/// - release(data, location)

namespace hh {
namespace comm {
namespace tool {

enum class MemoryManagerAllocatedMode {
  Wait, //< get is blocking and waits for the memory to be available
  Dynamic, //< allocates new memory if none is available
  Fail, //< return nullptr directly if no memory is availble
};

template <typename T>
struct SingleTypeMemoryManager {
  SingleTypeMemoryManager() = default;

  virtual ~SingleTypeMemoryManager() = default;

  virtual std::shared_ptr<T> allocate(MemoryManagerAllocatedMode mode = MemoryManagerAllocatedMode::Fail,
                                      std::source_location       loc = std::source_location::current())
      = 0;

  virtual void release(std::shared_ptr<T> &&data, std::source_location loc = std::source_location::current()) = 0;

  virtual std::string extraPrintingInformation() const {
    return "";
  };
};

template <typename T>
class SingleTypeMemoryManagerAbstraction {
public:
  SingleTypeMemoryManagerAbstraction() = default;

  SingleTypeMemoryManagerAbstraction(SingleTypeMemoryManager<T> *mmi)
      : mmi_(mmi) {}

  std::shared_ptr<T> allocate(MemoryManagerAllocatedMode mode, std::source_location loc) {
    return this->mmi_->allocate(mode, loc);
  }

  void release(std::shared_ptr<T> &&data, std::source_location loc) {
    this->mmi_->release(std::move(data), loc);
  }

  std::string extraPrintingInformation() const {
    return this->mmi_->extraPrintingInformation();
  }

private:
  SingleTypeMemoryManager<T> *mmi_ = nullptr;
};

template <typename... Types>
struct MemoryManager : SingleTypeMemoryManagerAbstraction<Types>... {
  MemoryManager(SingleTypeMemoryManager<Types> *...mmis)
      : SingleTypeMemoryManagerAbstraction<Types>(mmis)... {}

  template <typename MMI>
  MemoryManager(MMI *mmi)
      : MemoryManager(static_cast<SingleTypeMemoryManager<Types> *>(mmi)...) {}

  template <typename MMI>
  MemoryManager(std::shared_ptr<MMI> mmi)
      : MemoryManager(mmi.get()) {}

  template <typename T>
  std::shared_ptr<T> allocate(MemoryManagerAllocatedMode mode = MemoryManagerAllocatedMode::Fail,
                              std::source_location       loc = std::source_location::current()) {
    return static_cast<SingleTypeMemoryManagerAbstraction<T> *>(this)->allocate(mode, loc);
  }

  template <typename T>
  void release(std::shared_ptr<T> &&data, std::source_location loc = std::source_location::current()) {
    static_cast<SingleTypeMemoryManagerAbstraction<T> *>(this)->release(std::move(data), loc);
  }

  std::string extraPrintingInformation(std::string const &eol = "\\l") const {
    std::string infos = "MemoryManager: {" + eol;
    ([&] { infos.append("    " + static_cast<SingleTypeMemoryManagerAbstraction<Types> const*>(this)->extraPrintingInformation() + eol); }(),
     ...);
    return infos + "}" + eol;
  }
};

template <typename TM>
struct memory_manager_from_type_map {
  using type = void;
};

template <typename... Types>
struct memory_manager_from_type_map<TypeMap<Types...>> {
  using type = MemoryManager<Types...>;
};

template <typename TM>
using memory_manager_from_type_map_t = typename memory_manager_from_type_map<TM>::type;

static_assert(
    std::is_same_v<memory_manager_from_type_map_t<TypeMap<int, float, double>>, MemoryManager<int, float, double>>,
    "memory_manager_from_type_map_t has the wrong value");

} // end namespace tool
} // end namespace comm
} // end namespace hh

#endif
