add_subdirectory(cpp_odbc)
add_subdirectory(turbodbc)
add_subdirectory(turbodbc_python)
add_subdirectory(turbodbc_numpy)

find_package(Arrow)
if (ARROW_FOUND)
  add_subdirectory(turbodbc_arrow)
endif()
