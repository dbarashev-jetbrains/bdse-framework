## How to create a pull request

1. Every project has its own repository with the name consisting of a word `project` concatenated with the project number, e.g. `project1`. 
All pull requests should be targeted at the `main` branch in the project repository.
1. The list of tasks is maintained in the ["master" project repository](https://github.com/bdse-class-2025/framework/issues). 
Every task has a number in its title, e.g. "Task #1: kvnode storing data in PostgreSQL"). 

     When you work on a task number K, please commit your code into a task branch, with a name matching the pattern `task<N>-<mnemonics>` 
  where `<mnemonics>` is a short description of a changes in the branch or your nickname. Some examples of acceptable branch names:  
`task1-add-get-put` ("we add get and put method implementations for the task №1") or `task1-dbarashev` ("dbarashev's solution of task №1')
1. Please try to minimize modifications of the framework code. *Don't edit* the files where you see `// PLEASE DON'T EDIT THIS FILE` comment at the first line.
The reason is that such files usually declare interface for communication between different parts of KVaS or between
the client and KVaS servers. If you need to edit other parts of the framework code, it is better to discuss such 
changes in the chat.
1. When your code is ready, and you pushed it to the GitHub, you need to create a pull request from the task branch into the `main` branch of your project. 
I will appreciate if you link a pull request with the issue that it is related to (see Linked issues section on the screenshot below).

![image](https://user-images.githubusercontent.com/2028330/154058661-e9ec7680-aaab-4912-a505-f66069fc0901.png)
 
## How to review

When your pull request is ready, it needs to be reviewed by the teachers and optionally your fellow students. 
You may optionally participate in code review and get some additional credits for that. The code review guidelines
are as follows:

1. Check out the code and run `gradle build`.
1. Run KVaS in a single-node and multi-node setup and make sure that the nodes start.
1. Check that you actually use the implemented extensions, not the demo ones.
1. Run the load test and make sure that it completes normally with the expected results.
1. Read the code and write comments:
   1. Keep in mind that the goal of code review is to make code better. Showing off, insulting each other, etc. are 
      not the goals. Be kind, constructive and polite.
   1. The topic of this course are distributed storage systems, not programming in general and particularly not 
      programming in Kotlin. It is okay if something is implemented the way you don't like and doesn't look very
      elegant. Unless you see really weird code, please don't spend your time on improving your fellow Kotlin 
      programming skills.
   1. Focus on the algorithm/protocol logic, possible faulty corner cases, possible concurrency issues, etc.

## How to respond to code review

1. Read the comments.
1. If you agree with the reviewer, change the code. If you don't agree, discuss in the comments.
1. **PLEASE ALWAYS REACT TO ALL COMMENTS**. If you changed the code, replay with "done" or resolve the comment. 
   If some comment is "orphaned" without any reaction, we may not be sure that you have seen it or have considered 
   it and changed the code.
1. Push the changed code and notify your reviewer.


