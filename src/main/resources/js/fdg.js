var $id = {
    svg: d3.select('#$id svg'),
    nodes: d3.select('#$id svg g.nodes').selectAll('circle'),
    links: d3.select('#$id svg g.links').selectAll('line'),
    width: $width,
    height: $height,
    selectedCounter: 0,
    categoricalColor: d3.scale.category10().domain(d3.range(10)),
    force: d3.layout.force()
        .gravity(0.08)
        .distance(100)
        .charge(-70)
        .size([$width, $height]),
    getFill: function (d) {
        if (typeof d.hsl != 'undefined')
            return d3.hsl(d.hsl[0], d.hsl[1], d.hsl[2]);
        else if (typeof d.hue != 'undefined' && d.hue >= 0)
            return d3.hsl(d.hue, 1, 0.5);
        else
            return null;
    },
    getStroke: function (d) {
        if (typeof d.group != 'undefined' && d.group >= 0)
            return $id.categoricalColor(d.group);
        else
            return null;
    },
    onmouseover: function (node) {
        if ($id.selectedCounter > 0 && !node.selected)
            d3.select(this).style('opacity', null)

        $onMouseover
    },
    onmouseout: function (node) {
        if (!node.selected && $id.selectedCounter > 0)
            d3.select(this).style('opacity', .5)

        $onMouseout
    },
    onclicked: function (node) {
        if (node.selected) $id.selectedCounter -= 1;
        else $id.selectedCounter += 1;

        node.selected = !node.selected;

        if (node.selected && $id.selectedCounter == 1)
            $id.nodes
                .filter(function (n) { return !n.selected; })
                .style('opacity', .5);
        else if ($id.selectedCounter <= 0)
            $id.nodes.style('opacity', null);

        $onClicked
    },
    onupdate: function (data) {
        $onUpdate
    },
    bucket: document.getElementById('$dataBucketId')
};

var scope = angular.element($id.bucket).scope();

scope.$watch('$dataBucketId', function (newVal, oldVal) {
    if (typeof newVal == 'undefined') {
        console.log('Undefined data');
    } else {
        try {
            let data = JSON.parse(newVal);
            fdgSetData($id, data.nodes, data.links);
        } catch (e) {
            console.log(e);
        }
    }
});

function fdgSetData(fdg, newNodes, newLinks) {
    fdg.force.stop();
    let node = fdg.nodes;
    let oldNodes = node.data();
    let nodeCount = newNodes.length;

    for (let i = nodeCount - 1; i >= 0; i--) {
        let nn = newNodes[i];
        let on = oldNodes.find(function (d) {
            return d !== 'undefined' && nn.id == d.id;
        });

        if (on) {
            newNodes[i].x = on.x;
            newNodes[i].y = on.y;
            newNodes[i].selected = on.selected;
        }
    }


    let link = fdg.links.data(newLinks)
        .attr('stroke', function (d) {
            if (typeof d.hsl === 'undefined') return null;
            else return d3.hsl(d.hsl[0], d.hsl[1], d.hsl[2]);
         })
        .style('stroke-opacity', function (d) { return d.opacity; })
        .style('stroke-width', function (d) { return d.width; });
    link.exit().remove();
    link.enter().append('line')
        .attr('stroke', function (d) {
            if (typeof d.hsl === 'undefined') return null;
            else return d3.hsl(d.hsl[0], d.hsl[1], d.hsl[2]);
         })
        .style('stroke-opacity', function (d) { return d.opacity; })
        .style('stroke-width', function (d) { return d.width; });

    node = node.data(newNodes, function (d) { return d.id; });
    node.exit().remove();
    node.enter().append('circle')
        .attr('r', function (d) { return d.radius; })
        .attr('stroke', fdg.getStroke)
        .attr('fill', fdg.getFill)
        .call(fdg.force.drag)
        .on('mouseover', fdg.onmouseover)
        .on('mouseout', fdg.onmouseout)
        .on('click', fdg.onclicked)
        .append('title')
        .text(function (d) { return (typeof d.hint === 'undefined')? d.id : d.hint; });

    node
        .attr('r', function (d) { return d.radius; })
        .attr('stroke', fdg.getStroke)
        .attr('fill', fdg.getFill)
        .style('opacity', null)
        .select('title')
        .text(function (d) { return (typeof d.hint === 'undefined')? d.id : d.hint; });


    if (fdg.selectedCounter > 0)
        node
            .filter(function (d) { return !d.selected; })
            .style('opacity', '.3')

    fdg.links = link;
    fdg.nodes = node;
    fdg.force
        .nodes(newNodes)
        .links(newLinks)
        .linkDistance(function (d) { return d.distance + d.source.radius  + d.target.radius; })
        .alpha(.05)
        .on('tick', tick)
        .start();

    function tick() {
        link
            .attr('x1', function (d) { return d.source.x; })
            .attr('y1', function (d) { return d.source.y; })
            .attr('x2', function (d) { return d.target.x; })
            .attr('y2', function (d) { return d.target.y; });

        node
            .attr('cx', function (d) { return d.x = Math.max(d.radius, Math.min(fdg.width - d.radius, d.x)); })
            .attr('cy', function (d) { return d.y = Math.max(d.radius, Math.min(fdg.height - d.radius, d.y)); });
    }

    fdg.onupdate(newNodes);
}
