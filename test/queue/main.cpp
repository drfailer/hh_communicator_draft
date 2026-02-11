#include "../common/utest.h"
#include "../../src/communicator/tool/queue.hpp"


UTest(add) {
    Queue<int> queue1(0);
    uassert_equal(queue1.size(), (size_t)0);
    for (int j = 1; j <= 5; ++j) queue1.add(j);
    uassert_equal(queue1.size(), (size_t)5);

    int i = 1;
    for (auto elt : queue1) {
        uassert_equal(elt, i++);
    }

    Queue<int> queue2(3);
    uassert_equal(queue2.size(), (size_t)0);
    for (int j = 1; j <= 5; ++j) queue2.add(j);
    uassert_equal(queue2.size(), (size_t)5);

    i = 1;
    for (auto elt : queue2) {
        uassert_equal(elt, i++);
    }
}

UTest(remove) {
    Queue<int> queue;
    int i;

    // test remove the beginning and the end
    uassert_equal(queue.remove(queue.begin()), queue.end());
    uassert_equal(queue.remove(queue.end()), queue.end());

    // prepare the queue and test the size
    uassert_equal(queue.size(), (size_t)0);
    for (i = 0; i < 10; ++i) {
        queue.add(i);
    }
    uassert_equal(queue.size(), (size_t)10);

    // remove the even elements and test the size
    i = 0;
    for (auto it = queue.begin(); it != queue.end();) {
        if (i % 2 == 0) {
            it = queue.remove(it);
        } else {
            ++it;
        }
        i += 1;
    }
    uassert_equal(queue.size(), (size_t)5);

    // make sure all the remaining elements are odd
    i = 1;
    for (auto elt : queue) {
        uassert_equal(elt, i);
        i += 2;
    }

    // remove all the elements and test
    for (auto it = queue.begin(); it != queue.end();) {
        it = queue.remove(it);
    }
    uassert_equal(queue.size(), (size_t)0);
    for ([[maybe_unused]] auto elt : queue) {
        uassert(false);
    }
}

UTest(remove_and_clear) {
    Queue<int> queue;

    for (int i = 0; i < 10; ++i) queue.add(i);
    uassert_equal(queue.size(), (size_t)10);

    // Remove even elements
    int count = 0;
    for (auto it = queue.begin(); it != queue.end();) {
        if (count % 2 == 0) {
            it = queue.remove(it);
        } else {
            ++it;
        }
        count++;
    }
    uassert_equal(queue.size(), (size_t)5);

    // Test clear
    queue.clear();
    uassert_equal(queue.size(), (size_t)0);
    uassert_equal(queue.begin(), queue.end());
}

UTest(copy_method) {
    Queue<int> original;
    for (int i = 0; i < 5; ++i) original.add(i);

    // Test the custom copy method
    Queue<int> copied = original.copy();
    uassert_equal(copied.size(), (size_t)5);

    auto it_orig = original.begin();
    auto it_copy = copied.begin();
    while (it_orig != original.end()) {
        uassert_equal(*it_orig, *it_copy);
        ++it_orig;
        ++it_copy;
    }

    // Ensure they are independent
    copied.add(100);
    uassert_equal(original.size(), (size_t)5);
    uassert_equal(copied.size(), (size_t)6);
}

UTest(move_semantics) {
    Queue<int> source;
    for (int i = 0; i < 3; ++i) source.add(i);

    // Move constructor
    Queue<int> moved_to(std::move(source));
    uassert_equal(moved_to.size(), (size_t)3);

    // Move assignment
    Queue<int> move_assign;
    move_assign = std::move(moved_to);
    uassert_equal(move_assign.size(), (size_t)3);
}

UTest(const_iterator) {
    Queue<int> queue;
    queue.add(10);
    queue.add(20);

    const Queue<int>& c_queue = queue;

    // Testing const_iterator compilation and basic traversal
    int sum = 0;
    for (Queue<int>::const_iterator it = c_queue.begin(); it != c_queue.end(); ++it) {
        sum += *it;
    }
    uassert_equal(sum, 30);

    // Test that we can't modify via const_iterator (Compile-time check)
    // *c_queue.begin() = 5; // This should fail to compile if uncommented
}

int main(int , char **) {
    utest_start();
    urun_test(add);
    urun_test(remove);
    urun_test(remove_and_clear);
    urun_test(copy_method);
    urun_test(move_semantics);
    urun_test(const_iterator);
    utest_end();
    return 0;
}
