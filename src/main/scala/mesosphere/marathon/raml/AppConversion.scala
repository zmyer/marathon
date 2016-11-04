package mesosphere.marathon
package raml

import mesosphere.marathon.state.AppDefinition

trait AppConversion {

  // FIXME: implement me
  implicit val appWriter: Writes[AppDefinition, App] = Writes { app =>
    App(id = app.id.toString)
  }
}
