class DataHub {
  constructor(id, input_bucket_id, output_bucket_id){
    this.id = id;
    this.input_bucket = document.getElementById(input_bucket_id);
    this.output_bucket = document.getElementById(output_bucket_id);

    this.data = [];
    this.selected = [];
    this.listeners = [];

    if (this.input_bucket != null && typeof this.input_bucket != 'undefined') {
      this.watchBucket(this.input_bucket);
    }
  }

  notify(listener) {
    if (typeof listener.listenUpdate !== 'undefined') {
      listener.listenUpdate(this.data);
    }
    this.listeners.push(listener);
    return this;
  }

  notifyUpdate(data) {
    for (var i = 0; i < this.listeners.length; i++) {
      const listener = this.listeners[i];
      if (typeof listener.listenUpdate !== 'undefined') {
        listener.listenUpdate(data);
      }
    }
    return this;
  }

  notifyMouseover(d, caller) {
    for (var i = 0; i < this.listeners.length; i++) {
      const listener = this.listeners[i];
      if (listener.id != caller.id && typeof listener.listenMouseover !== 'undefined') {
        listener.listenMouseover(d, caller);
      }
    }
  }

  notifyMouseout(d, caller) {
    for (var i = 0; i < this.listeners.length; i++) {
      const listener = this.listeners[i];
      if (listener.id != caller.id && typeof listener.listenMouseout !== 'undefined') {
        listener.listenMouseout(d, caller);
      }
    }
  }

  notifyMouseclick(d, caller) {
    let index = this.selected.findIndex((e) => {
      return d.id == e.id;
    });

    if (index >= 0) {
      this.selected.splice(index, 1);
    } else {
      this.selected.push(d);
    }

    this.saveToOutputBucket(this.selected.map((e) => { return e.id }));

    for (var i = 0; i < this.listeners.length; i++) {
      const listener = this.listeners[i];
      if (listener.id != caller.id && typeof listener.listenMouseclick !== 'undefined') {
        listener.listenMouseclick(d, caller, this.selected);
      }
    }
  }

  notifySelected(selectedElems, caller) {
    this.selected = selectedElems;

    for (var i = 0; i < this.listeners.length; i++) {
      const listener = this.listeners[i];
      if (listener.id != caller.id && typeof listener.listenSelected !== 'undefined') {
        listener.listenSelected(this.selected, caller);
      }
    }

    this.saveToOutputBucket(this.selected.map((e) => { return e.id }));
  }

  saveToOutputBucket(data) {
    if (this.output_bucket != null && typeof this.output_bucket != 'undefined') {
      let scope = angular.element(this.output_bucket).scope();
      scope[this.output_bucket.id] = JSON.stringify(data);
    }
  }

  watchBucket(bucket) {
    let self = this;
    let scope = angular.element(bucket).scope();

    scope.$watch(bucket.id, (newVal, oldVal) => {
      if (typeof newVal !== 'undefined') {
        try {
          self.data = JSON.parse(newVal);
          self.notifyUpdate(self.data);
        } catch (e) {
          console.log(e);
        }
      }
    });
  }
}
