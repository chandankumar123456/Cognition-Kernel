use crossterm::{
    event::{self, Event, KeyCode, KeyModifiers},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use ratatui::{
    backend::CrosstermBackend,
    layout::{Constraint, Direction, Layout},
    style::{Color, Modifier, Style},
    widgets::{Block, Borders, Cell, Paragraph, Row, Table},
    Terminal,
};
use std::io;
use std::time::Duration;
use ck_memory::store::Store;

pub async fn run_watch(db_path: &str) -> io::Result<()> {
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let result = watch_loop(&mut terminal, db_path).await;

    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    terminal.show_cursor()?;
    result
}

async fn watch_loop(
    terminal: &mut Terminal<CrosstermBackend<io::Stdout>>,
    db_path: &str,
) -> io::Result<()> {
    loop {
        if event::poll(Duration::from_millis(100))? {
            if let Event::Key(key) = event::read()? {
                match key.code {
                    KeyCode::Char('q') => break,
                    KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => break,
                    _ => {}
                }
            }
        }

        let (tasks, events) = match Store::open(db_path) {
            Ok(store) => {
                let tasks = store.list_tasks().unwrap_or_default();
                let events = store.recent_events(20).unwrap_or_default();
                (tasks, events)
            }
            Err(_) => (vec![], vec![]),
        };

        terminal.draw(|f| {
            let chunks = Layout::default()
                .direction(Direction::Vertical)
                .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
                .split(f.area());

            // Tasks panel
            let header = Row::new(vec![
                Cell::from("ID").style(Style::default().add_modifier(Modifier::BOLD)),
                Cell::from("Goal").style(Style::default().add_modifier(Modifier::BOLD)),
                Cell::from("Status").style(Style::default().add_modifier(Modifier::BOLD)),
                Cell::from("Step").style(Style::default().add_modifier(Modifier::BOLD)),
            ]);

            let rows: Vec<Row> = tasks.iter().map(|t| {
                let status_style = match t.status {
                    ck_memory::store::TaskStatus::Completed => Style::default().fg(Color::Green),
                    ck_memory::store::TaskStatus::Failed => Style::default().fg(Color::Red),
                    ck_memory::store::TaskStatus::Escalated => Style::default().fg(Color::Yellow),
                    ck_memory::store::TaskStatus::Executing => Style::default().fg(Color::Cyan),
                    _ => Style::default().fg(Color::White),
                };
                let id_short = t.id.chars().take(8).collect::<String>();
                let goal_short: String = t.goal.chars().take(40).collect();
                let goal_display = if t.goal.len() > 40 { format!("{}...", goal_short) } else { goal_short };
                Row::new(vec![
                    Cell::from(id_short),
                    Cell::from(goal_display),
                    Cell::from(format!("{:?}", t.status)).style(status_style),
                    Cell::from(t.current_step.to_string()),
                ])
            }).collect();

            let task_table = Table::new(
                rows,
                [Constraint::Length(10), Constraint::Min(20), Constraint::Length(12), Constraint::Length(6)],
            )
            .header(header)
            .block(Block::default().borders(Borders::ALL).title(" Tasks "));

            f.render_widget(task_table, chunks[0]);

            // Events panel
            let event_lines: Vec<ratatui::text::Line> = events.iter().map(|e| {
                let task_short: String = e.task_id.chars().take(8).collect();
                let ts_sec = e.timestamp / 1000;
                ratatui::text::Line::from(format!("[{}] {:30} | {}", ts_sec, e.event_type, task_short))
            }).collect();

            let event_widget = Paragraph::new(event_lines)
                .block(Block::default().borders(Borders::ALL).title(" Events (press q to quit) "));

            f.render_widget(event_widget, chunks[1]);
        })?;

        tokio::time::sleep(tokio::time::Duration::from_millis(400)).await;
    }
    Ok(())
}
