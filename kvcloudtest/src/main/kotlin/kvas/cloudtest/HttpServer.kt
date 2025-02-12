package kvas.cloudtest

import io.ktor.http.*
import io.ktor.server.application.*
import io.ktor.server.response.*
import io.ktor.server.sessions.*
import io.ktor.server.routing.*
import io.ktor.server.engine.embeddedServer
import io.ktor.server.netty.Netty
import io.ktor.server.auth.*
import kotlinx.serialization.Serializable

@Serializable
data class UserSession(val name: String)

fun main() {
    embeddedServer(Netty, port = 8080) {
    install(Sessions) {
      cookie<UserSession>("USER_SESSION")
    }

        
        install(Authentication) {
            form("auth-form") {
                userParamName = "username"
                passwordParamName = "password"
                validate { credentials ->
                    if (credentials.name == "user" && credentials.password == "password") {
                        UserIdPrincipal(credentials.name)
                    } else {
                        null
                    }
                }
                challenge {
                    call.respond(HttpStatusCode.Unauthorized, "Invalid username or password")
                }
            }
        }

        routing {
            authenticate("auth-form") {
                post("/login") {
                    val principal = call.principal<UserIdPrincipal>()
                    if (principal != null) {
                        call.sessions.set(UserSession(principal.name))
                        call.respondText("Welcome, ${principal.name}!")
                    } else {
                        call.respond(HttpStatusCode.Unauthorized, "Invalid login attempt.")
                    }
                }
            }

            get("/") {
                val session = call.sessions.get<UserSession>()
                if (session != null) {
                    call.respondText("Hello, ${session.name}!")
                } else {
                    call.respondText("Hello, World!")
                }
            }
            
            get("/login") {
                call.respondText(
                    """
                        <!DOCTYPE html>
                        <html>
                        <head>
                            <title>Login</title>
                        </head>
                        <body>
                            <form action="/login" method="post">
                                <label for="username">Username:</label>
                                <input type="text" id="username" name="username">
                                <br>
                                <label for="password">Password:</label>
                                <input type="password" id="password" name="password">
                                <br>
                                <button type="submit">Login</button>
                            </form>
                        </body>
                        </html>
                    """.trimIndent(),
                    ContentType.Text.Html
                )
            }
            
            
            post("/logout") {
                call.sessions.clear<UserSession>()
                call.respondText("You have been logged out.")
            }
        }
    }.start(wait = true)
}
