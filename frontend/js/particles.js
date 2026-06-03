/**
 * 粒子星空动画
 * - 漂浮粒子 + 连线效果
 * - 鼠标交互：粒子跟随鼠标产生拖尾/排斥
 */
(function() {
    'use strict';

    const canvas = document.getElementById('particles-canvas');
    const ctx = canvas.getContext('2d');

    let width, height;
    let particles = [];
    let mouse = { x: null, y: null, radius: 120 };
    let animationId = null;

    const PARTICLE_COUNT = 120;
    const CONNECTION_DIST = 140;
    const PARTICLE_SPEED = 0.3;
    const PARTICLE_COLORS = [
        'rgba(167, 139, 250,',  // purple
        'rgba(96, 165, 250,',   // blue
        'rgba(244, 114, 182,',  // pink
        'rgba(52, 211, 153,',   // green
    ];

    function resize() {
        width = window.innerWidth;
        height = window.innerHeight;
        canvas.width = width;
        canvas.height = height;
    }

    function createParticle() {
        const color = PARTICLE_COLORS[Math.floor(Math.random() * PARTICLE_COLORS.length)];
        return {
            x: Math.random() * width,
            y: Math.random() * height,
            vx: (Math.random() - 0.5) * PARTICLE_SPEED,
            vy: (Math.random() - 0.5) * PARTICLE_SPEED,
            size: Math.random() * 2.5 + 1,
            color: color,
            alpha: Math.random() * 0.5 + 0.2,
            baseAlpha: Math.random() * 0.5 + 0.2,
        };
    }

    function initParticles() {
        particles = [];
        for (let i = 0; i < PARTICLE_COUNT; i++) {
            particles.push(createParticle());
        }
    }

    function draw() {
        ctx.clearRect(0, 0, width, height);

        for (let i = 0; i < particles.length; i++) {
            const p = particles[i];

            // 鼠标交互 - 排斥效果
            if (mouse.x !== null && mouse.y !== null) {
                const dx = p.x - mouse.x;
                const dy = p.y - mouse.y;
                const dist = Math.sqrt(dx * dx + dy * dy);
                if (dist < mouse.radius) {
                    const force = (mouse.radius - dist) / mouse.radius;
                    const angle = Math.atan2(dy, dx);
                    p.vx += Math.cos(angle) * force * 0.5;
                    p.vy += Math.sin(angle) * force * 0.5;
                }
            }

            // 更新位置
            p.x += p.vx;
            p.y += p.vy;

            // 边界回弹
            if (p.x < 0 || p.x > width) p.vx *= -1;
            if (p.y < 0 || p.y > height) p.vy *= -1;

            // 速度阻尼
            p.vx *= 0.99;
            p.vy *= 0.99;

            // 限制最大速度
            const speed = Math.sqrt(p.vx * p.vx + p.vy * p.vy);
            if (speed > 1.5) {
                p.vx = (p.vx / speed) * 1.5;
                p.vy = (p.vy / speed) * 1.5;
            }

            // 绘制粒子
            ctx.beginPath();
            ctx.arc(p.x, p.y, p.size, 0, Math.PI * 2);
            ctx.fillStyle = p.color + p.alpha + ')';
            ctx.fill();

            // 粒子发光光晕
            ctx.beginPath();
            ctx.arc(p.x, p.y, p.size * 3, 0, Math.PI * 2);
            ctx.fillStyle = p.color + (p.alpha * 0.15) + ')';
            ctx.fill();

            // 粒子间连线
            for (let j = i + 1; j < particles.length; j++) {
                const p2 = particles[j];
                const dx = p.x - p2.x;
                const dy = p.y - p2.y;
                const dist = Math.sqrt(dx * dx + dy * dy);

                if (dist < CONNECTION_DIST) {
                    const lineAlpha = (1 - dist / CONNECTION_DIST) * 0.15;
                    ctx.beginPath();
                    ctx.moveTo(p.x, p.y);
                    ctx.lineTo(p2.x, p2.y);
                    ctx.strokeStyle = `rgba(167, 139, 250, ${lineAlpha})`;
                    ctx.lineWidth = 0.8;
                    ctx.stroke();
                }
            }
        }

        animationId = requestAnimationFrame(draw);
    }

    function onMouseMove(e) {
        mouse.x = e.clientX;
        mouse.y = e.clientY;
    }

    function onMouseLeave() {
        mouse.x = null;
        mouse.y = null;
    }

    function onResize() {
        resize();
    }

    function start() {
        resize();
        initParticles();
        window.addEventListener('resize', onResize);
        window.addEventListener('mousemove', onMouseMove);
        window.addEventListener('mouseleave', onMouseLeave);
        draw();
    }

    // 页面可见性变化时暂停/恢复，省资源
    document.addEventListener('visibilitychange', function() {
        if (document.hidden) {
            if (animationId) {
                cancelAnimationFrame(animationId);
                animationId = null;
            }
        } else {
            if (!animationId) {
                draw();
            }
        }
    });

    // DOM 加载完成后启动
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', start);
    } else {
        start();
    }
})();
