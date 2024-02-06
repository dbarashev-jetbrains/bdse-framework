# How to create a pull request

Every project has its own repository with the name consisting of a word `project` concatenated with the project number, e.g. `project1`. 
All pull requests should be targeted at the `main` branch in the project repository.


The list of tasks is maintained in the ["master" project repository](https://github.com/bdse-class-2024/project0/issues). 
Every task has a number in its title, e.g. "Task #1: kvnode storing data in PostgreSQL"). 

When you work on a task number K, please commit your code into a task branch, with a name matching the pattern `task<N>-<mnemonics>` where `<mnemonics>` is a short description of a changes in the branch or your nickname. Some examples of acceptable branch names:  
`task1-add-proto` ("we add protocol buffer definitions for the task №1") or `task1-dbarashev` ("dbarashev's solution of task №1')

PLEASE DON'T TOUCH the files created by me in your projects. Just don't touch them, even if you 
want it very much. Don't format them either. If there is indeed a strong reason to change them, 
let me know and we will agree how to introduce the changes.

Teh reason is that I will change the framework code, and if we do it concurrently then conflict resolving and merging will take a lot of my time.

When tou code is ready, and you pushed it to the GitHub, you need to create a pull request from the task branch into the `main` branch of your project. 

I will appreciate if you link a pull request with the issue that it is related to.

![image](https://user-images.githubusercontent.com/2028330/154058661-e9ec7680-aaab-4912-a505-f66069fc0901.png)

In the reviewers field set your team mate as a reviewer.
