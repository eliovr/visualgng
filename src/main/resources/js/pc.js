var options = {
    height: 500,        // svg height. Width is defined based on the number of features.
    padding: {          // svg paddings.
        top: 10,
        bottom: 10,
        left: 10
    },
    axis_span: 100,      // distance between each axis.
    axis_width: 0,      // width of each axis (might vary based on boxplot_width).
    boxplot_width: 6,   // width of each box-plot.
    boxplot_margin: 1,
    colors: d3.scale.category10().domain(d3.range(10)),
    paint_boxplots: false,
    hasClusters: false
}

var series_style = { 'stroke': 'gray', 'fill': 'none', 'stroke-opacity': '0.5' },
    ticks_line_style = { 'stroke': '#78797c', 'fill': 'none' },
    ticks_text_style = { 'stroke': 'none', 'fill': 'black', 'font-size': '12px' },
    axis_bg_style = { 'fill': 'darkgray', 'opacity': '.4', 'stroke': 'black' },
    axis_labels_style = { 'fill': '#999', 'font-size': '13px' },
    axis_filters_style = { 'cursor': 'move' },
    boxplot_style = { 'stroke': '#34363a' },
    boxplot_box_style = { 'fill-opacity': '.4' },
    boxplot_median_style = { 'stroke-width': '2' };

// === Esqueleton ==
// -- svg
var svg = d3.select('#$id svg');

// -- series
svg.append('g')
    .attr('class', 'series')
    .style(series_style);

// -- axis
var axis = svg.append('g').attr('class', 'axis');

// -- ticks
axis.append('g')
    .attr('class', 'ticks')
    .style(ticks_line_style);

// -- labels
axis.append('g')
    .attr('class', 'labels')
    .style(axis_labels_style);

// -- boxplots
axis.append('g')
    .attr('class', 'box-plots')
    .style(boxplot_style);

// -- filters
var filters = axis.append('g')
    .attr('class', 'filters')
    .style(axis_filters_style);

filters.append('g').attr('class', 'lower');
filters.append('g').attr('class', 'upper');

// === PC object
var $id = {
    options: options,
    features: $features,
    svg: svg,
    categoricalColor: d3.scale.category10().domain(d3.range(10)),
    filters: { lower: [], upper: [] },
    getSeries: function () {
        return d3.select('#$id svg g.series').selectAll('path');
    },
    isFiltered: function (d) {
        let lower = this.filters.lower,
            upper = this.filters.upper;

        for (let i = 0; i < lower.length; i++) {
            const l = lower[i], u = upper[i], x = d.data[i];
            if (x > u.value || x < l.value)
                return true;
        }

        return false;
    },
    isNotFiltered: function (d) { return !$id.isFiltered(d); },
    getNonFiltered: function () {
        return $id.getSeries().filter($id.isNotFiltered);
    },
    onFilter: function (elems) {
        $onFilterScript
    },
    getPath: function (d) {
        let self = $id,
            o = self.options,
            values = d.data,
            features = self.features,
            f = features[0],
            first_y = f.scale(values[0]),
            path = 'M' + o.padding.left + ' ' + first_y + ' L' + (f.x + o.axis_width) + ' ' + first_y;

        for (let i = 1; i < values.length; i++) {
            f = features[i];
            const y = f.scale(values[i]);

            path += ' L' + f.x + ' ' + y + ' L' + (f.x + o.axis_width) + ' ' + y;
        }

        return path;
    },
    getStroke: function (d) {
        if ($id.isFiltered(d)) return 'lightgray';

        if (typeof d.group != 'undefined' && d.group >= 0)
            return $id.options.colors(d.group);
        else if (typeof d.hsl != 'undefined')
            return d3.hsl(d.hsl[0], d.hsl[1], d.hsl[2]);
        else if (typeof d.hue != 'undefined' && d.hue >= 0)
            return d3.hsl(d.hue, 1, .5);
        else return null;
    },
    paint: function () {
        let self = $id,
            features = self.features,
            data = self.getSeries().data(),
            o = self.options,
            longest_label = 0;

        for (let i = 0; i < features.length; i++) {
            longest_label = Math.max(longest_label, features[i].name.length);
        }

        o.axis_width = 0;
        o.labels_height = longest_label * 5.5;
        o.labels_y = o.height - o.labels_height - o.padding.bottom;
        o.axis_height = o.labels_y - 15;

        for (let i = 0; i < features.length; i++) {
            const f = features[i];
            f.x = (i * (o.axis_span + o.axis_width)) + o.padding.left + .5;
            f.scale = d3.scale.linear()
                .domain([f.min, f.max])
                .range([o.axis_height, o.padding.top]);

            if (o.paint_boxplots) {
                for (let j = 0; j < f.clusters.length; j++) {
                    f.clusters[j].x = f.x + (j * o.boxplot_width + o.boxplot_margin);
                }
            } 
                
        }

        o.width = features.length * (o.axis_span + o.axis_width) + o.padding.left + (o.labels_height / 2);

        self.svg
            .attr("height", o.height)
            .attr("width", o.width)
            .style('min-width', o.width + 'px');

        self.paintAxis(o, features);
        self.paintLabels(o, features);
        self.paintFilters(o, features);
    },
    paintAxis: function (o, features) {
        let self = $id,
            ticks = self.svg.select('g.axis g.ticks');

        ticks.selectAll('*').remove();

        for (let i = 0; i < features.length; i++) {
            let f = features[i];

            let tick = d3.svg.axis()
                .scale(f.scale)
                .orient('right')
                .ticks(5)
                .tickFormat(d3.format('s'));

            ticks
                .append('g')
                .attr('transform', 'translate(' + (f.x + o.axis_width + o.boxplot_margin) + ',0)')
                .call(tick);

            ticks.append('rect')
                .attr('x', f.x)
                .attr('y', o.padding.top)
                .attr('width', o.axis_width)
                .attr('height', o.axis_height - o.padding.top)
                .style(axis_bg_style);
        }

        ticks.selectAll('text').style(ticks_text_style);
        ticks.selectAll('.tick').style({ 'display': 'none' });

        self.svg
            .on('mouseover', function () {
                ticks.selectAll('.tick').style({ 'display': null })
            })
            .on('mouseout', function () {
                ticks.selectAll('.tick').style({ 'display': 'none' })
            });
    },
    paintLabels: function (o, features) {
        let labels = $id.svg.select('g.axis g.labels')
            .selectAll('text')
            .data(features)
            .attr('x', function (d) { return d.x })
            .attr('y', o.labels_y)
            .attr('transform', function (d) {
                return 'rotate(45,' + d.x + ', ' + o.labels_y + ')';
            })
            .text(function (d) { return d.name; });

        labels.enter().append('text')
            .attr('x', function (d) { return d.x })
            .attr('y', o.labels_y)
            .attr('transform', function (d) {
                return 'rotate(45,' + d.x + ', ' + o.labels_y + ')';
            })
            .text(function (d) { return d.name; });
    },
    paintFilters: function (o, features) {
        let self = $id,
            filters = self.svg.select('g.axis g.filters'),
            drag = d3.behavior.drag()
                .origin(function (d) { return d; })
                .on("drag", function (d) {
                    let y = Math.min(Math.max(d3.event.y, d.min_y), d.max_y);

                    if (d.y !== y) {
                        this.setAttribute('cy', y);
                        d.y = y;
                        d.value = d.scale(y);

                        // update stroke colors.
                        self.getSeries().attr('stroke', self.getStroke);
                        // trigger onfilter event.
                        self.onFilter(self.getNonFiltered());
                    }
                });

        self.filters.lower = features.map(function (d) {
            return {
                x: d.x + o.axis_width,
                y: o.axis_height,
                min_y: o.padding.top,
                max_y: o.axis_height,
                scale: d3.scale.linear()
                    .domain([o.axis_height, o.padding.top])
                    .range([d.min, d.max]),
                value: d.min
            };
        });

        self.filters.upper = features.map(function (d) {
            return {
                x: d.x + o.axis_width,
                y: o.padding.top,
                min_y: o.padding.top,
                max_y: o.axis_height,
                scale: d3.scale.linear()
                    .domain([o.axis_height, o.padding.top])
                    .range([d.min, d.max]),
                value: d.max
            };
        });

        let lower = filters.select('g.lower')
            .selectAll('circle')
            .data(self.filters.lower)
            .attr('cx', function (d) { return d.x; })
            .attr('cy', function (d) { return d.y; });

        lower.enter().append('circle')
            .attr('cx', function (d) { return d.x; })
            .attr('cy', function (d) { return d.y; })
            .attr('r', 3)
            .call(drag);

        let upper = filters.select('g.upper')
            .selectAll('circle')
            .data(self.filters.upper)
            .attr('cx', function (d) { return d.x; })
            .attr('cy', function (d) { return d.y; });

        upper.enter().append('circle')
            .attr('cx', function (d) { return d.x; })
            .attr('cy', function (d) { return d.y; })
            .attr('r', 3)
            .call(drag);


    },
    paintBoxplots: function (o, features, redraw) {
        let self = $id;
        if (typeof redraw === 'undefined')
            redraw = false;

        if (redraw) 
            self.svg.select('g.axis g.box-plots')
                .selectAll('g')
                .remove();

        let axi = self.svg.select('g.axis g.box-plots')
            .selectAll('g')
            .data(features);

        // ----------- Boxes -----------
        if (o.hasClusters) {
            axi = (redraw ? axi.enter().append('g').attr('class', 'axi') : axi.selectAll('g.axi'))
                .selectAll('g')
                .data(function (d) { return d.clusters; });
        }

        let box = (redraw ?
            axi.enter().append('g').attr('class', 'box-plot') :
            axi.selectAll('g.box-plot'));

        (redraw ? box.append('rect') : box.selectAll('rect'))
            .attr('x', function (d) { return d.x; })
            .attr('y', function (d) { return d.feature.scale(d.q3); })
            .attr('width', o.boxplot_width)
            .attr('height', function (d) { return d.feature.scale(d.q1) - d.feature.scale(d.q3); })
            .attr('fill', function (d) { return o.colors(d.id); })
            .style(boxplot_box_style);

        // ----------- Medians --------
        (redraw ? box.append('line') : box.selectAll('line'))
            .attr('class', 'median')
            .attr('x1', function (d) { return d.x; })
            .attr('y1', function (d) { return d.feature.scale(d.median); })
            .attr('x2', function (d) { return d.x + o.boxplot_width; })
            .attr('y2', function (d) { return d.feature.scale(d.median); })
            .style(boxplot_median_style);

        // --------- Whiskers ---------
        (redraw ? box.append('path') : box.selectAll('path'))
            .attr('class', 'whisker')
            .attr('d', function (d) {
                let scale = d.feature.scale,
                    lx = d.x,
                    rx = lx + o.boxplot_width,
                    mx = lx + (o.boxplot_width / 2),
                    y1 = scale(d.w2),
                    y2 = scale(d.q3),
                    y3 = scale(d.q1),
                    y4 = scale(d.w1);

                let path = 'M' + lx + ' ' + y1;
                path += ' H' + rx;
                path += ' M' + mx + ' ' + y1;
                path += ' V' + y2;
                path += ' M' + mx + ' ' + y3;
                path += ' V' + y4;
                path += ' M' + lx + ' ' + y4;
                path += ' H' + rx;

                return path;
            })
    },
    setData: function (data, redraw) {
        let self = $id;

        let series = self.getSeries().data(data)
            .attr('stroke-width', null)
            .attr('stroke-opacity', null)
            .attr('stroke', self.getStroke)
            .attr('d', self.getPath);

        series.exit().remove();

        series.enter().append('path')
            .attr('d', self.getPath)
            .attr('stroke', self.getStroke);

        series
            .filter(function (d) { return d.selected; })
            .attr('stroke-width', 3)
            .attr('stroke-opacity', 1);

    }
};

d3.select('input#boxplots')
    .on('change', function(){
        let self = $id;
        self.options.paint_boxplots = this.checked;
        self.paint();
        self.setData(self.getSeries().data(), true);
        // if (this.checked) self.paint_boxplots();
    });
$id.paint();

$onDisplayScript

// -------------- Bucket watch -----------

var bucket = document.getElementById('$dataBucketId');
var scope = angular.element(bucket).scope();

scope.$watch('$dataBucketId', function (newVal) {
    try {
        let data = JSON.parse(newVal);
        $id.setData(data);
    } catch (error) {
        console.log('Could not parse JSON data ' + newVal + ' [' + error + ']');
    }
});