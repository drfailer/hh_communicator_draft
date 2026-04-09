#ifndef COMMUNICATOR_TOOL_MEMORY_MANAGER
#define COMMUNICATOR_TOOL_MEMORY_MANAGER
#include "../tool/log.hpp"
#include <memory>
#include <source_location>
#include <type_traits>
#include <sstream>
#include <hedgehog.h>
#include <stdio.h>

/// The memory manager is used to allocated memory on the receiver end of the
/// communicator. It can also be used freely to control the amount of memory
/// allocated by a program.
///
/// A memory manager should implement the following functions:
/// - allocate(mode, location)
/// - release(data, location)

/// @brief Hedgehog namespace
namespace hh {
/// @brief Communicator namespace
namespace comm {
/// @brief Tools namespace
namespace tool {

/// @brief Allocation mode of the memory manager.
enum class MemoryManagerAllocateMode {
  Wait,    ///< get is blocking and waits for the memory to be available
  Dynamic, ///< allocates new memory if none is available
  Fail,    ///< return nullptr directly if no memory is availble
};

/******************************************************************************/
/*                          SingleTypeMemoryManager                           */
/******************************************************************************/

/// @brief Memory manager interface for a single type.
/// @tparam T Type of the managed memory.
template <typename T>
struct SingleTypeMemoryManager {
  /// @brief Default constructor.
  SingleTypeMemoryManager() = default;

  /// @brief Default destructor.
  virtual ~SingleTypeMemoryManager() = default;

  /// @brief Allocate an new element.
  /// @param mode Allocation mode (see MemoryManagerAllocateMode).
  /// @param loc  Source location of the allocation (used for debugging/profiling).
  /// @return New shared pointer to a data of type T.
  virtual std::shared_ptr<T> allocate(MemoryManagerAllocateMode mode = MemoryManagerAllocateMode::Fail,
                                      std::source_location      loc = std::source_location::current())
      = 0;

  /// @brief Release an element.
  /// @parma data Data to release.
  /// @param loc  Source location of the allocation (used for debugging/profiling).
  virtual void release(std::shared_ptr<T> &&data, std::source_location loc = std::source_location::current()) = 0;

  /// @brief  Used to allow the memory manager to output debugging/profiling
  ///         information in the dot file.
  /// @return String that contains the extra information.
  virtual std::string extraPrintingInformation() const { return ""; };
};

/******************************************************************************/
/*                     SingleTypeMemoryManagerAbstraction                     */
/******************************************************************************/

/// @brief Abstraction for the single type memory manager.
///
/// This pattern allows creating a memory manager that supports multiple types
/// from a type that inherits from multiple `SingleTypeMemoryManager`. The
/// reason why this abstraction is used instead of having custom memory
/// managers inherit directly from `MemoryManager<Types...>` is because this
/// allows generating generic default implementations common to multiple types.
/// For instance, one can inherit from multiple
/// "Default_SingleTypeMemoryManager<Types>..." where the default
/// implementation is the same for all the types, which is not possible to do
/// when inheriting directly from "MemoryManager<Types...>" because virtual
/// functions cannot be template.
///
/// @tparam T Type managed by the memory manager.
template <typename T>
class SingleTypeMemoryManagerAbstraction {
public:
  /// @brief Default constructor
  SingleTypeMemoryManagerAbstraction() = default;

  /// @brief Construction from `SingleTypeMemoryManager<T>*`.
  /// @param mmi Implementor of `SingleTypeMemoryManager<T>`.
  SingleTypeMemoryManagerAbstraction(SingleTypeMemoryManager<T> *mmi)
      : mmi_(mmi) {}

  /// @brief Initialize from `SingleTypeMemoryManager<T>*`.
  /// @param mmi Implementor of `SingleTypeMemoryManager<T>`.
  void initialize(SingleTypeMemoryManager<T> *mmi) {
    this->mmi_ = mmi;
  }

  /// @brief Call the `allocate` method of the implementor (throws a runtime
  ///        error when the implementor is nullptr).
  /// @param mode Allocation mode.
  /// @param loc  Souce location.
  /// @return Result of implementor->allocate.
  std::shared_ptr<T> allocate(MemoryManagerAllocateMode mode, std::source_location loc) {
    if (this->mmi_ == nullptr) {
      std::ostringstream oss;
      oss << "error: tried to allocated an element of type '" << hh::tool::typeToStr<T>()
          << "' at " << loc.file_name() << ":" << loc.line()
          << " using a non implemented memory manager.";
      throw std::runtime_error(oss.str());
    }
    return this->mmi_->allocate(mode, loc);
  }

  /// @brief Call the `release` method of the implementor (throws a runtime
  ///        error when the implementor is nullptr).
  /// @param mode Allocation mode.
  /// @param loc  Source location of the release.
  void release(std::shared_ptr<T> &&data, std::source_location loc) {
    if (this->mmi_ == nullptr) {
      std::ostringstream oss;
      oss << "error: tried to release an element of type '" << hh::tool::typeToStr<T>()
          << "' at " << loc.file_name() << ":" << loc.line()
          << " using a non implemented memory manager.";
      throw std::runtime_error(oss.str());
    }
    this->mmi_->release(std::move(data), loc);
  }

  /// @brief Call the `extraPrintingInformation` method of the implementor,
  ///        returns an empty string if the implementor is not set.
  /// @return Extra information to print i the dot file.
  std::string extraPrintingInformation() const {
    if (this->mmi_ == nullptr) {
      return "";
    }
    return this->mmi_->extraPrintingInformation();
  }

private:
  SingleTypeMemoryManager<T> *mmi_ = nullptr; ///< Pointer to the memory manager implementor.
};

/******************************************************************************/
/*                               MemoryManager                                */
/******************************************************************************/

template <typename MM, typename T>
concept HasMemoryManager = requires(MM *mm) {
    { mm->template getMemoryManager<T>() } -> std::same_as<SingleTypeMemoryManager<T> *>;
};

/// @brief Memory manager that can manage multiple types.
///
/// Custom memory managers should implement the `SingleTypeMemoryManager<T>`
/// interface instead of inheriting from `MemoryManager` (see explanation in
/// `SingleTypeMemoryManagerAbstraction` documentation).
///
/// @tparam Types List of supported types.
template <typename... Types>
struct MemoryManager final : SingleTypeMemoryManagerAbstraction<Types>... {
  /// @brief Default constructor for the memory manager.
  explicit MemoryManager() = default;

  /// @brief Initialize the bases SingleTypeMemoryManager for the types handled
  ///        by the given implementor.
  ///        Note: we suport 2 kinds of memory manager implementors:
  ///        - ones that inherit from SingleTypeMemoryManager for all the
  ///          supported types.
  ///        - ones that implemente the getMemoryManager<T>() method that
  ///          returns a pointer to a SingleTypeMemoryManager<T>.
  /// @tparam MM Type of the memory manager implementor.
  /// @param mm Pointer to a memory manger implementor.
  template <typename MM>
  void initialize(MM *mm) {
    (initializeForAType<Types>(mm), ...);
  }

  /// @brief Call the `allocate` method of the memory manager for the given type.
  /// @tparam T Type of the data to `allocate`.
  /// @param mode Allocation mode.
  /// @param loc  Source location.
  /// @return newly allocated data.
  template <typename T>
  std::shared_ptr<T> allocate(MemoryManagerAllocateMode mode = MemoryManagerAllocateMode::Fail,
                              std::source_location      loc = std::source_location::current()) {
    return SingleTypeMemoryManagerAbstraction<T>::allocate(mode, loc);
  }

  /// @brief Call the `release` method of the memory manager for the given type.
  /// @tparam T Type of the data to `release`.
  /// @param mode Allocation mode.
  /// @param loc  Source location.
  template <typename T>
  void release(std::shared_ptr<T> &&data, std::source_location loc = std::source_location::current()) {
    SingleTypeMemoryManagerAbstraction<T>::release(std::move(data), loc);
  }

  /// @brief Collect the extra printing information of the memory managers of
  ///        each type and return the final string.
  /// @tparam T Type of the data to `release`.
  /// @param mode Allocation mode.
  /// @param loc  Source location.
  std::string extraPrintingInformation(std::string const &eol = "\\l") const {
    std::string infos = "MemoryManager: {" + eol;
    ([&] { infos.append("    " + SingleTypeMemoryManagerAbstraction<Types>::extraPrintingInformation()+ eol); }(),...);
    return infos + "}" + eol;
  }

private:
  /// @brief Initialize the base SingleTypeMemoryManager for the type T.
  /// @tparam T Type for which we need to initialize the implementor.
  /// @param mm Pointer to a memory manger implementor (can be multi-typed).
  template <typename T>
  void initializeForAType(auto *mm) {
    using MM = std::remove_pointer_t<decltype(mm)>;
    if constexpr (std::is_base_of_v<SingleTypeMemoryManager<T>, MM>) {
      SingleTypeMemoryManagerAbstraction<T>::initialize(
              static_cast<SingleTypeMemoryManager<T> *>(mm));
    } else if constexpr (HasMemoryManager<MM, T>) {
      SingleTypeMemoryManagerAbstraction<T>::initialize(mm->template getMemoryManager<T>());
    } else {
      /* we don't throw an error here to allow using setMemoryManager multiple
       * times with different memory managers for different set of types. */
    }
  }
};

} // end namespace tool

} // end namespace comm

} // end namespace hh

#endif
