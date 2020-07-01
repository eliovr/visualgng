class DataHub {
  constructor(id, input_bucket_id, output_bucket_id){
    this.id = id;
    this.input_bucket = document.getElementById(input_bucket_id);
    this.output_bucket = document.getElementById(output_bucket_id);

    this.data = null;
    this.selected = [];
    this.listeners = [];

    this.selectedFeature = -1;

    if (this.input_bucket != null && typeof this.input_bucket != 'undefined') {
      this.watchBucket(this.input_bucket);
    }
  }

  notify(listener) {
    if (typeof listener.listenUpdate !== 'undefined' && this.data != null) {
      listener.listenUpdate(this.data);
    }
    this.listeners.push(listener);
    return this;
  }

  setSelectedFeature(featureIndex) {
    this.selectedFeature = featureIndex;
    if (featureIndex >= 0) {
      this.enrichData(this.data);
    } else {
      let nodes = this.data.nodes;
      for (var i = 0; i < nodes.length; i++) {
        const n = nodes[i];
        if ('hsl' in n) delete n.hsl;
      }
    }
    this.notifyUpdate(this.data);
  }

  notifyUpdate(data) {
    this.data = data;

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
    this.selected = selectedElems || [];

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

  enrichData(data) {
    if (this.selectedFeature < 0) return;

    let nodes = data.nodes;
    let sf = this.selectedFeature;

    let stats = {
      'min': Number.MAX_SAFE_INTEGER,
      'max': Number.MIN_SAFE_INTEGER,
      'sum': 0,
      'count': 0
    }

    for (var i = 0; i < nodes.length; i++) {
      const x = nodes[i].data[sf];
      stats.min = Math.min(stats.min, x);
      stats.max = Math.max(stats.max, x);
      stats.sum += x;
      stats.count += 1;
    }

    stats.avg = stats.sum / stats.count;

    for (var i = 0; i < nodes.length; i++) {
      const n = nodes[i];
      n.hsl = this.spenceHSL(stats, n.data[sf]);
    }
  }

  spenceHSL(stats, x) {
    let scaler = d3.scale.linear();
    let lightness = .78 - scaler
      .domain([stats.min, stats.max])
      .range([.59, .78])(x) + .59

    let saturation = scaler.domain([0, stats.max]).range([0, .95])(x);
    if (stats.min >= 0) {
      if (x > stats.avg)
        saturation = scaler.domain([stats.avg, stats.max]).range([0, .95])(x)
      else
        saturation = scaler.domain([0, stats.avg - stats.min]).range([0, .95])(stats.avg - x)
    }
    else if (x < 0)
      saturation = scaler.domain([0, Math.abs(stats.min)]).range([0, .95])(Math.abs(x))

    let hue = 0;
    if (x < stats.avg) hue = 346
    else if (x > stats.avg) hue = 34

    return d3.hsl(hue, saturation, lightness);
  }

  watchBucket(bucket) {
    let self = this;
    let scope = angular.element(bucket).scope();

    scope.$watch(bucket.id, (newVal, oldVal) => {
      if (typeof newVal !== 'undefined') {
        try {
          let data = JSON.parse(newVal);
          self.enrichData(data);
          self.notifyUpdate(data);
        } catch (e) {
          console.log(e);
        }
      }
    });
  }
}
