package se.his.sail.zeppelin

import org.apache.zeppelin.display.angular.AbstractAngularElem
import org.apache.zeppelin.display.angular.notebookscope.AngularElem._
import org.apache.zeppelin.display.angular.notebookscope._
import org.slf4j.LoggerFactory

import scala.xml.{Elem, Null, UnprefixedAttribute, Text => XMLText}

object AngularElement {
  private val suffix = "vis_"
  private var id: Int = 0

  def nextID: String = {
    id += 1
    s"$suffix$id"
  }
}

abstract class AngularElement [T] {
  protected val logger = LoggerFactory.getLogger(classOf[AngularElement[T]])

  val id: String = AngularElement.nextID
  var elem: AbstractAngularElem

  def get: T =
    AngularModel(id)().asInstanceOf[T]

  def set(v: T): Unit =
    AngularModel(id, v)


  def setAttribute(attr: String, value: String): this.type = {
    elem = elem % new UnprefixedAttribute(attr, value, Null)
    this
  }

  def setOnClickListener(f: (T) => Unit): this.type = {
    elem = elem.onClick(() => f(get))
    this
  }

  def setOnChangeListener(f: (T) => Unit): this.type = {
    elem = elem.onChange(() => f(get))
    this
  }

  def setOnClickScript(script: String): this.type = {
    setAttribute("onclick", script)
    this
  }

  def setOnChangeScript(script: String): this.type = {
    setAttribute("onchange", script)
    this
  }


  def jsGetElementById: String =
    s"document.getElementById('$id')"

  def jsStyleDisplay(display: String = "block"): String =
    s"$jsGetElementById.style.display='$display';"

  def jsStyleVisibility(visible: Boolean): String = {
    val visibility: String = visible match {
      case true => "visible"
      case _ => "hidden"
    }

    s"$jsGetElementById.style.visibility='$visibility';"
  }

  override def toString: String =
    elem.toString()
}

class InputNumber (value: Double = 0.0) extends AngularElement[Double] {

  var elem: AbstractAngularElem =
    <input id={id} type="number" class="form-control"></input>.model(id, value)

  /** There seems to be a bug in the inbuilt scala casting
    * (http://www.scala-lang.org/old/node/4431)
    * so handle it this way.
    */
  override def get: Double = {
    val value = AngularModel(id)()

    try {

      value.toString.toDouble

    } catch {

      case e: Exception =>
        logger.error(s"Error casting $value to Double", e)
        -1.0

    }
  }

  def setMax(n: Double): InputNumber = {
    setAttribute("max", n.toString)
    this
  }

  def setMin(n: Double): InputNumber = {
    setAttribute("min", n.toString)
    this
  }

  def setStep(n: Double): InputNumber = {
    setAttribute("step", n.toString)
    this
  }

}

class InputText (value: String = "", placeholder: String = "") extends AngularElement[String] {
  var elem: AbstractAngularElem =
    <input id={id} type="text" class="form-control" placeholder={placeholder}></input>.model(id, value)
}

class InputHidden (value: String = "") extends AngularElement[String] {
  var elem: AbstractAngularElem =
    <input id={id} type="hidden"></input>.model(id, value)
}

class Button (value: String, className: String = "btn btn-default btn-sm") extends AngularElement[String] {
  override var elem: AbstractAngularElem =
    <button id={id} type="button" class={className}>{{{{{id}}}}}</button>.model(id, value)

  def setOnClickListener(f: () => Unit): Button = {
    elem = elem.onClick(() => f())
    this
  }

  def setDisabled(): Button = {
    setAttribute("disabled", "true")
    this
  }

  def jsDisabled(disabled: Boolean = true): String =
    s"$jsGetElementById.disabled=$disabled;"

}

class Checkbox (checked: Boolean = false) extends AngularElement[Boolean] {
  var elem: AbstractAngularElem =
    <input id={id} type="checkbox"></input>.model(id, checked)
}

class Select (options: Array[String], selectedIndex: Int = 0) extends AngularElement[String] {
  var elem: AbstractAngularElem = {
    val selectOptions: Array[Elem] = options.map(t => <option value={t}>{t}</option>)

    <select id={id} class="form-control">{selectOptions}</select>.model(id, options(selectedIndex))
  }
}

class Text (text: String = "") extends AngularElement[String] {
  var elem: AbstractAngularElem =
    <span>{{{{{id}}}}}</span>.model(id, text)
}

class Alert (text: String) extends AngularElement[String] {
  var elem: AbstractAngularElem =
    <div class="alert alert-warning" role="alert">{{{{{id}}}}}</div>.model(id, text)
}

class ScriptText(data: String) extends XMLText(data) {
  override def buildString(sb: StringBuilder): StringBuilder = sb.append(data)
}